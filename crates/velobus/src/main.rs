mod protocol;
mod rpc;
mod store;
mod wal;

use bytes::Bytes;
use protocol::{
    decode_request, envelope, error_body, Error, Request, MAX_FRAME, OVERLOADED, PROTOCOL, STORAGE,
    UNAUTHORIZED,
};
use rpc::Rpc;
use std::collections::HashSet;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use store::Store;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

const HELLO_TIMEOUT: Duration = Duration::from_secs(60);
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

struct Config {
    listen: SocketAddr,
    data_dir: PathBuf,
    memory: bool,
    max_retained_bytes: u64,
    max_wal_bytes: u64,
    max_connections: usize,
    max_rpc_calls: usize,
    max_rpc_bytes: u64,
    token: String,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>, token: String) -> Result<Self, String> {
        let mut config = Self {
            listen: "127.0.0.1:7447".parse().unwrap(),
            data_dir: "./data/velobus".into(),
            memory: false,
            max_retained_bytes: 64 * 1024 * 1024,
            max_wal_bytes: 128 * 1024 * 1024,
            max_connections: 64,
            max_rpc_calls: 1024,
            max_rpc_bytes: 16 * 1024 * 1024,
            token,
        };
        let mut explicit_data_dir = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if arg == "--memory" {
                config.memory = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--listen" => {
                    config.listen = value.parse().map_err(|_| {
                        "--listen must be an IP address and port, e.g. 127.0.0.1:7447"
                    })?
                }
                "--data-dir" => {
                    if value.is_empty() {
                        return Err("--data-dir cannot be empty".into());
                    }
                    config.data_dir = value.into();
                    explicit_data_dir = true;
                }
                "--max-retained-bytes" => config.max_retained_bytes = parse_budget(&value, 1)?,
                "--max-wal-bytes" => config.max_wal_bytes = parse_budget(&value, 8)?,
                "--max-rpc-bytes" => config.max_rpc_bytes = parse_budget(&value, 1)?,
                "--max-rpc-calls" => {
                    config.max_rpc_calls = value.parse().map_err(|_| "invalid --max-rpc-calls")?;
                    if !(1..=1_000_000).contains(&config.max_rpc_calls) {
                        return Err("--max-rpc-calls must be 1..1000000".into());
                    }
                }
                "--max-connections" => {
                    config.max_connections =
                        value.parse().map_err(|_| "invalid --max-connections")?;
                    if !(1..=4096).contains(&config.max_connections) {
                        return Err("--max-connections must be 1..4096".into());
                    }
                }
                _ => return Err(format!("unknown option {arg}")),
            }
        }
        if config.memory && explicit_data_dir {
            return Err("--memory conflicts with --data-dir".into());
        }
        if !config.listen.ip().is_loopback() && config.token.is_empty() {
            return Err("VELOBUS_TOKEN is required for a non-loopback listener".into());
        }
        if config.token.len() > u16::MAX as usize {
            return Err("VELOBUS_TOKEN exceeds protocol limit".into());
        }
        Ok(config)
    }
}

fn parse_budget(value: &str, minimum: u64) -> Result<u64, String> {
    let budget = value
        .parse::<u64>()
        .map_err(|_| "byte budgets must be unsigned decimal integers")?;
    let maximum = (1u64 << 40).min(usize::MAX as u64);
    if !(minimum..=maximum).contains(&budget) {
        return Err(format!("byte budget must be {minimum}..{maximum}"));
    }
    Ok(budget)
}

fn help() {
    println!("VeloBus {} — experimental single-node request/reply intermediary\n\nUsage: velobus [options]\n  --listen IP:PORT             Default 127.0.0.1:7447; port 0 selects a free port\n  --data-dir PATH              Default ./data/velobus; durable checksummed WAL\n  --memory                    Explicit volatile mode (conflicts with --data-dir)\n  --max-retained-bytes N       Default 67108864; no silent eviction\n  --max-wal-bytes N            Default 134217728\n  --max-connections N          Default 64, range 1..4096\n  --max-rpc-calls N            Default 1024 waiting/running calls\n  --max-rpc-bytes N            Default 16777216 accounted RPC bytes\n  --help                      Show this help\n  --version                   Show version\n\nVELOBUS_TOKEN: optional on loopback; required otherwise.\nNo TLS: use a trusted network or encrypted tunnel for remote access.\nMemory history and sequence numbers reset when the process restarts.", env!("CARGO_PKG_VERSION"));
}

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        help();
        return;
    }
    if args.iter().any(|arg| arg == "--version") {
        println!("velobus {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let token = match std::env::var("VELOBUS_TOKEN") {
        Ok(token) => token,
        Err(std::env::VarError::NotPresent) => String::new(),
        Err(_) => {
            eprintln!("velobus: VELOBUS_TOKEN must be valid UTF-8");
            std::process::exit(1);
        }
    };
    match Config::parse(args, token) {
        Ok(config) => {
            if let Err(error) = serve(config).await {
                eprintln!("velobus: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("velobus: {error}; use --help");
            std::process::exit(1);
        }
    }
}

async fn serve(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let store = if config.memory {
        Store::memory(config.max_retained_bytes, config.max_wal_bytes)
    } else {
        Store::disk(
            &config.data_dir,
            config.max_retained_bytes,
            config.max_wal_bytes,
        )?
    };
    let store = Arc::new(Mutex::new(store));
    let rpc = Rpc::new(config.max_rpc_calls, config.max_rpc_bytes);
    let listener = TcpListener::bind(config.listen).await?;
    let semaphore = Arc::new(Semaphore::new(config.max_connections));
    let token = Arc::new(config.token);
    println!(
        "{}",
        serde_json::json!({ "event": "ready", "address": listener.local_addr()?.to_string(), "mode": if config.memory { "memory" } else { "disk" } })
    );
    std::io::stdout().flush()?;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut connection_id = 0u64;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = semaphore.clone().try_acquire_owned() else { drop(stream); continue; };
                let store = store.clone();
                let token = token.clone();
                let semaphore = semaphore.clone();
                let max_connections = config.max_connections;
                let rpc = rpc.clone();
                connection_id = connection_id.checked_add(1).ok_or("connection identifier space exhausted")?;
                let id = connection_id;
                tokio::spawn(async move {
                    let _permit = permit;
                    // Malformed/closed clients are routine; do not log payloads or credentials.
                    let _ = connection(stream, store, token, semaphore, max_connections, rpc, id).await;
                });
            }
            result = &mut shutdown => { result?; break; }
        }
    }
    Ok(())
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

fn valid_length(length: usize) -> bool {
    (5..=MAX_FRAME).contains(&length)
}

async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    authenticated: bool,
) -> Result<(u8, u32, Bytes), std::io::Error> {
    let mut prefix = [0; 4];
    if authenticated {
        // An established client may legitimately wait indefinitely between requests.
        stream.read_exact(&mut prefix).await?;
    } else {
        timeout(HELLO_TIMEOUT, stream.read_exact(&mut prefix)).await??;
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if !valid_length(length) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut data = vec![0; length];
    timeout(BODY_TIMEOUT, stream.read_exact(&mut data)).await??;
    let bytes = Bytes::from(data);
    let request_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
    if request_id == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "zero request id",
        ));
    }
    Ok((bytes[0], request_id, bytes.slice(5..)))
}

async fn write_response<W: AsyncWrite + Unpin>(
    stream: &mut W,
    opcode: u8,
    request_id: u32,
    body: &[u8],
) -> Result<(), std::io::Error> {
    timeout(
        WRITE_TIMEOUT,
        stream.write_all(&response_frame(opcode, request_id, body)),
    )
    .await??;
    Ok(())
}

fn response_frame(opcode: u8, request_id: u32, body: &[u8]) -> Vec<u8> {
    if body.len() > MAX_FRAME - 5 {
        return envelope(
            255,
            request_id,
            &error_body(&Error::new(
                protocol::LIMIT,
                "response exceeds maximum frame size",
            )),
        );
    }
    envelope(opcode, request_id, body)
}

const MAX_IN_FLIGHT: usize = 32;

struct Response {
    opcode: u8,
    request_id: u32,
    body: Vec<u8>,
    _permit: Option<OwnedSemaphorePermit>,
    written: Option<oneshot::Sender<()>>,
}

struct ConnectionCleanup {
    rpc: Arc<Rpc>,
    connection_id: u64,
    reader: tokio::task::AbortHandle,
    writer: tokio::task::AbortHandle,
}

impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        self.rpc.disconnect(self.connection_id);
        self.reader.abort();
        self.writer.abort();
    }
}

async fn connection(
    mut stream: TcpStream,
    store: Arc<Mutex<Store>>,
    token: Arc<String>,
    connections: Arc<Semaphore>,
    max_connections: usize,
    rpc: Arc<Rpc>,
    connection_id: u64,
) -> Result<(), std::io::Error> {
    stream.set_nodelay(true)?;
    let (opcode, request_id, body) = read_frame(&mut stream, false).await?;
    let hello = decode_request(opcode, body);
    let error = match hello {
        Ok(Request::Hello {
            version: 1,
            token: supplied,
        }) if bool::from(supplied.as_bytes().ct_eq(token.as_bytes())) => None,
        Ok(Request::Hello { version, .. }) if version != 1 => {
            Some(Error::new(PROTOCOL, "unsupported protocol version"))
        }
        Err(error) => Some(error),
        _ => Some(Error::new(
            UNAUTHORIZED,
            "HELLO with valid credentials is required",
        )),
    };
    if let Some(error) = error {
        write_response(&mut stream, 255, request_id, &error_body(&error)).await?;
        return Ok(());
    }
    write_response(&mut stream, opcode | 128, request_id, &1u16.to_be_bytes()).await?;
    let (mut reader, mut writer) = stream.into_split();
    let (frames_tx, mut frames_rx) = mpsc::channel(1);
    // A dedicated reader retains partial frames across unrelated operation completions.
    let mut reader_task = tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut reader, true).await?;
            if frames_tx.send(frame).await.is_err() {
                return Ok::<(), std::io::Error>(());
            }
        }
    });
    let ids = Arc::new(Mutex::new(HashSet::new()));
    let writer_ids = ids.clone();
    let (responses_tx, mut responses_rx) = mpsc::channel::<Response>(1);
    let mut writer_task = tokio::spawn(async move {
        while let Some(response) = responses_rx.recv().await {
            write_response(
                &mut writer,
                response.opcode,
                response.request_id,
                &response.body,
            )
            .await?;
            writer_ids.lock().unwrap().remove(&response.request_id);
            if let Some(written) = response.written {
                let _ = written.send(());
            }
        }
        Ok::<(), std::io::Error>(())
    });
    rpc.connected(connection_id);
    let _cleanup = ConnectionCleanup {
        rpc: rpc.clone(),
        connection_id,
        reader: reader_task.abort_handle(),
        writer: writer_task.abort_handle(),
    };
    let permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let mut operations = JoinSet::new();
    let result = async {
        loop {
            tokio::select! {
                result = &mut reader_task => { return result.map_err(std::io::Error::other)?; }
                result = &mut writer_task => { return result.map_err(std::io::Error::other)?; }
                result = operations.join_next(), if !operations.is_empty() => {
                    if let Some(Err(error)) = result { return Err(std::io::Error::other(error)); }
                }
                frame = frames_rx.recv() => {
                    let Some((opcode, request_id, body)) = frame else { return Ok(()); };
                    // Completed task records cannot grow with the lifetime of a busy connection.
                    while let Some(result) = operations.try_join_next() {
                        if let Err(error) = result { return Err(std::io::Error::other(error)); }
                    }
                    let duplicate = !ids.lock().unwrap().insert(request_id);
                    if duplicate {
                        let (written, receipt) = oneshot::channel();
                        let _ = responses_tx.send(Response { opcode: 255, request_id, body: error_body(&Error::new(PROTOCOL, "duplicate in-flight request identifier")), _permit: None, written: Some(written) }).await;
                        let _ = receipt.await;
                        return Ok(());
                    }
                    let permit = permits.clone().try_acquire_owned();
                    let request = decode_request(opcode, body);
                    let error = match &request {
                        Ok(Request::Hello { .. }) => Some(Error::new(PROTOCOL, "HELLO is allowed only once")),
                        Err(error) => Some(Error::new(error.code, error.message.clone())),
                        _ if permit.is_err() => Some(Error::new(OVERLOADED, "connection has 32 in-flight operations")),
                        _ => None,
                    };
                    if let Some(error) = error {
                        let close = error.code == PROTOCOL;
                        let (written, receipt) = oneshot::channel();
                        if responses_tx.send(Response { opcode: 255, request_id, body: error_body(&error), _permit: permit.ok(), written: Some(written) }).await.is_err() { return Ok(()); }
                        let _ = receipt.await;
                        if close { return Ok(()); }
                        continue;
                    }
                    let permit = permit.unwrap();
                    let request = request.unwrap();
                    let store = store.clone();
                    let rpc = rpc.clone();
                    let sender = responses_tx.clone();
                    let current_connections = max_connections - connections.available_permits();
                    operations.spawn(async move {
                        let result = dispatch(request, store, rpc, connection_id, current_connections).await;
                        let (opcode, body) = match result { Ok(body) => (opcode | 128, body), Err(error) => (255, error_body(&error)) };
                        let _ = sender.send(Response { opcode, request_id, body, _permit: Some(permit), written: None }).await;
                    });
                }
            }
        }
    }.await;
    // Scheduler cleanup is immediate even if an operation is waiting on a deadline/TAKE.
    rpc.disconnect(connection_id);
    operations.abort_all();
    reader_task.abort();
    writer_task.abort();
    while operations.join_next().await.is_some() {}
    result
}

async fn dispatch(
    request: Request,
    store: Arc<Mutex<Store>>,
    rpc: Arc<Rpc>,
    connection: u64,
    connections: usize,
) -> Result<Vec<u8>, Error> {
    match request {
        Request::Ping => Ok(Vec::new()),
        Request::Register {
            route,
            concurrency,
            queue_limit,
        } => {
            rpc.register(connection, route, concurrency, queue_limit)?;
            Ok(Vec::new())
        }
        Request::Rpc {
            route,
            timeout_ms,
            payload,
        } => {
            let result = rpc
                .request(connection, route, timeout_ms, payload)?
                .wait()
                .await?;
            let mut body = Vec::with_capacity(4 + result.len());
            body.extend_from_slice(&(result.len() as u32).to_be_bytes());
            body.extend_from_slice(&result);
            Ok(body)
        }
        Request::Take { wait_ms } => Ok(rpc
            .take(connection, wait_ms)
            .await?
            .map(|job| job.encode())
            .unwrap_or_else(|| vec![0])),
        Request::Complete {
            call_id,
            failed,
            payload,
        } => {
            rpc.complete(connection, call_id, failed, payload)?;
            Ok(Vec::new())
        }
        request => {
            let rpc_stats = matches!(request, Request::Stats).then(|| rpc.stats());
            tokio::task::spawn_blocking(move || {
                let mut store = store
                    .lock()
                    .map_err(|_| Error::new(STORAGE, "store lock poisoned; restart required"))?;
                match request {
                    Request::Publish(records) => store.publish(records),
                    Request::Fetch(fetch) => store.fetch(fetch).map(|result| result.encode()),
                    Request::Stats => {
                        let mut stats: serde_json::Value =
                            serde_json::from_slice(&store.stats(connections))
                                .expect("valid store stats");
                        stats["rpc"] = rpc_stats.expect("RPC stats captured for STATS");
                        Ok(serde_json::to_vec(&stats).expect("valid merged stats"))
                    }
                    _ => Err(Error::new(PROTOCOL, "invalid request state")),
                }
            })
            .await
            .unwrap_or_else(|_| Err(Error::new(STORAGE, "store worker failed; restart required")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_cli_validation() {
        let config = Config::parse([], String::new()).unwrap();
        assert_eq!(
            config.listen,
            "127.0.0.1:7447".parse::<SocketAddr>().unwrap()
        );
        assert!(!config.memory);
        for args in [
            vec!["--memory", "--data-dir", "/tmp/test"],
            vec!["--listen", "0.0.0.0:7447"],
            vec!["--max-connections", "0"],
            vec!["--max-retained-bytes", "0"],
            vec!["--max-wal-bytes", "7"],
            vec!["--max-retained-bytes", "18446744073709551615"],
        ] {
            assert!(Config::parse(args.into_iter().map(str::to_string), String::new()).is_err());
        }
        assert!(Config::parse(
            ["--listen", "0.0.0.0:7447"].map(str::to_string),
            "secret".into()
        )
        .is_ok());
    }

    #[test]
    fn envelopes_are_bounded_before_allocation() {
        assert!(!valid_length(0));
        assert!(!valid_length(4));
        assert!(valid_length(5));
        assert!(valid_length(MAX_FRAME));
        assert!(!valid_length(MAX_FRAME + 1));
        assert!(!valid_length(u32::MAX as usize));
        let oversized = response_frame(132, 17, &vec![0; MAX_FRAME]);
        assert!(oversized.len() < 100);
        assert_eq!(oversized[4], 255);
        assert_eq!(u32::from_be_bytes(oversized[5..9].try_into().unwrap()), 17);
        assert_eq!(
            u16::from_be_bytes(oversized[9..11].try_into().unwrap()),
            protocol::LIMIT
        );
    }

    #[tokio::test]
    async fn zero_id_and_oversize_envelope_disconnect() {
        for bad in [
            0u32.to_be_bytes().to_vec(),
            (MAX_FRAME as u32 + 1).to_be_bytes().to_vec(),
            envelope(5, 0, &[]),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let worker = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                assert!(read_frame(&mut stream, false).await.is_err());
            });
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(&bad).await.unwrap();
            worker.await.unwrap();
        }
    }

    #[tokio::test]
    async fn partial_frame_survives_rpc_deadline_and_connection_cleanup_removes_route() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rpc = Rpc::new(16, 1 << 20);
        let server_rpc = rpc.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            connection(
                stream,
                Arc::new(Mutex::new(Store::memory(1 << 20, 1 << 20))),
                Arc::new(String::new()),
                Arc::new(Semaphore::new(0)),
                1,
                server_rpc,
                1,
            )
            .await
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(&envelope(1, 1, &[0, 1, 0, 0]))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client, true).await.unwrap().0, 129);
        let mut registration = Vec::new();
        protocol::put_string(&mut registration, "users.get");
        registration.extend_from_slice(&[0, 1, 0, 4]);
        client
            .write_all(&envelope(6, 2, &registration))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client, true).await.unwrap().0, 134);
        let mut request = Vec::new();
        protocol::put_string(&mut request, "users.get");
        request.extend_from_slice(&20u32.to_be_bytes());
        request.extend_from_slice(&0u32.to_be_bytes());
        client.write_all(&envelope(7, 3, &request)).await.unwrap();
        let ping = envelope(5, 4, &[]);
        client.write_all(&ping[..2]).await.unwrap();
        let (opcode, id, body) = timeout(Duration::from_secs(1), read_frame(&mut client, true))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((opcode, id), (255, 3));
        assert_eq!(
            u16::from_be_bytes(body[..2].try_into().unwrap()),
            protocol::DEADLINE_EXCEEDED
        );
        client.write_all(&ping[2..]).await.unwrap();
        let (opcode, id, _) = timeout(Duration::from_secs(1), read_frame(&mut client, true))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((opcode, id), (133, 4));
        drop(client);
        let _ = timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rpc.stats()["queued"], 0);
        assert!(rpc.stats()["routes"].as_array().unwrap().is_empty());
    }
}
