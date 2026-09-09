use crate::protocol::{
    Error, DEADLINE_EXCEEDED, HANDLER_ERROR, INVALID, NO_SERVICE, OVERLOADED, SERVICE_UNAVAILABLE,
    UNAUTHORIZED,
};
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::time::{timeout_at, Instant};

type Reply = Result<Bytes, Error>;

struct Route {
    workers: Vec<u64>,
    queue: VecDeque<u64>,
    queue_limit: usize,
}

struct Worker {
    route: String,
    concurrency: usize,
    running: usize,
    pending_takes: usize,
    notify: Arc<Notify>,
}

struct Call {
    caller: u64,
    route: String,
    owner: Option<u64>,
    deadline: Instant,
    payload: Bytes,
    response: Option<oneshot::Sender<Reply>>,
    retained_bytes: u64,
}

struct State {
    connections: HashSet<u64>,
    routes: HashMap<String, Route>,
    workers: HashMap<u64, Worker>,
    calls: HashMap<u64, Call>,
    next_id: u64,
    retained_bytes: u64,
    max_calls: usize,
    max_bytes: u64,
    accepted: u64,
    completed: u64,
    rejected: u64,
    timed_out: u64,
}

pub struct Rpc {
    state: Mutex<State>,
}

pub struct Ticket {
    rpc: Arc<Rpc>,
    call_id: u64,
    deadline: Instant,
    receiver: Option<oneshot::Receiver<Reply>>,
}

impl Ticket {
    pub async fn wait(mut self) -> Reply {
        match timeout_at(
            self.deadline,
            self.receiver.take().expect("ticket consumed once"),
        )
        .await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => Err(Error::new(
                SERVICE_UNAVAILABLE,
                "RPC response channel closed",
            )),
            Err(_) => {
                self.rpc.expire(self.call_id);
                Err(deadline_error())
            }
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.rpc.cancel(self.call_id);
    }
}

struct TakeGuard {
    rpc: Arc<Rpc>,
    worker_id: u64,
}

impl Drop for TakeGuard {
    fn drop(&mut self) {
        if let Some(worker) = self.rpc.lock().workers.get_mut(&self.worker_id) {
            worker.pending_takes = worker.pending_takes.saturating_sub(1);
        }
    }
}

pub struct Job {
    pub call_id: u64,
    pub remaining_ms: u32,
    pub payload: Bytes,
}

impl Job {
    pub fn encode(self) -> Vec<u8> {
        let mut body = Vec::with_capacity(17 + self.payload.len());
        body.push(1);
        body.extend_from_slice(&self.call_id.to_be_bytes());
        body.extend_from_slice(&self.remaining_ms.to_be_bytes());
        body.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        body.extend_from_slice(&self.payload);
        body
    }
}

fn deadline_error() -> Error {
    Error::new(
        DEADLINE_EXCEEDED,
        "RPC deadline exceeded; started work may still finish",
    )
}

impl State {
    fn remove(&mut self, id: u64) -> Option<Call> {
        let call = self.calls.remove(&id)?;
        self.retained_bytes -= call.retained_bytes;
        if let Some(owner) = call.owner {
            if let Some(worker) = self.workers.get_mut(&owner) {
                worker.running = worker.running.saturating_sub(1);
                worker.notify.notify_waiters();
            }
        } else if let Some(route) = self.routes.get_mut(&call.route) {
            route.queue.retain(|queued| *queued != id);
        }
        Some(call)
    }

    fn expire(&mut self, id: u64) {
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        if let Some(response) = call.response.take() {
            self.timed_out = self.timed_out.saturating_add(1);
            let _ = response.send(Err(deadline_error()));
        }
        if call.owner.is_none() {
            self.remove(id);
        }
    }

    fn expire_queued(&mut self, route: &str) {
        let now = Instant::now();
        let expired = self
            .routes
            .get(route)
            .into_iter()
            .flat_map(|route| &route.queue)
            .filter(|id| self.calls.get(id).is_some_and(|call| call.deadline <= now))
            .copied()
            .collect::<Vec<_>>();
        for id in expired {
            self.expire(id);
        }
    }

    fn notify_route(&self, name: &str) {
        if let Some(route) = self.routes.get(name) {
            for id in &route.workers {
                if let Some(worker) = self.workers.get(id) {
                    worker.notify.notify_waiters();
                }
            }
        }
    }
}

impl Rpc {
    pub fn new(max_calls: usize, max_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                connections: HashSet::new(),
                routes: HashMap::new(),
                workers: HashMap::new(),
                calls: HashMap::new(),
                next_id: 1,
                retained_bytes: 0,
                max_calls,
                max_bytes,
                accepted: 0,
                completed: 0,
                rejected: 0,
                timed_out: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("RPC scheduler invariant panic")
    }

    pub fn connected(&self, connection: u64) {
        self.lock().connections.insert(connection);
    }

    pub fn register(
        &self,
        worker_id: u64,
        name: String,
        concurrency: usize,
        queue_limit: usize,
    ) -> Result<(), Error> {
        let mut state = self.lock();
        if !state.connections.contains(&worker_id) {
            return Err(Error::new(
                SERVICE_UNAVAILABLE,
                "worker connection has closed",
            ));
        }
        if state.workers.contains_key(&worker_id) {
            return Err(Error::new(INVALID, "connection already registered a route"));
        }
        if let Some(route) = state.routes.get(&name) {
            if route.queue_limit != queue_limit {
                return Err(Error::new(
                    INVALID,
                    "workers for a route must agree on queueLimit",
                ));
            }
        }
        state
            .routes
            .entry(name.clone())
            .or_insert_with(|| Route {
                workers: Vec::new(),
                queue: VecDeque::new(),
                queue_limit,
            })
            .workers
            .push(worker_id);
        state.workers.insert(
            worker_id,
            Worker {
                route: name,
                concurrency,
                running: 0,
                pending_takes: 0,
                notify: Arc::new(Notify::new()),
            },
        );
        Ok(())
    }

    pub fn request(
        self: &Arc<Self>,
        caller: u64,
        route: String,
        timeout_ms: u32,
        payload: Bytes,
    ) -> Result<Ticket, Error> {
        let mut state = self.lock();
        if !state.connections.contains(&caller) {
            return Err(Error::new(
                SERVICE_UNAVAILABLE,
                "caller connection has closed",
            ));
        }
        if !state.routes.contains_key(&route) {
            state.rejected = state.rejected.saturating_add(1);
            return Err(Error::new(NO_SERVICE, "no worker registered for route"));
        }
        state.expire_queued(&route);
        // Includes the shared input frame, duplicate decoded route, map/queue growth and oneshot state.
        let retained_bytes = 512 + 2 * route.len() as u64 + payload.len() as u64;
        let route_state = &state.routes[&route];
        if route_state.queue.len() >= route_state.queue_limit
            || state.calls.len() >= state.max_calls
            || retained_bytes > state.max_bytes.saturating_sub(state.retained_bytes)
        {
            state.rejected = state.rejected.saturating_add(1);
            return Err(Error::new(
                OVERLOADED,
                "RPC admission budget is full; request was not accepted",
            ));
        }
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| Error::new(OVERLOADED, "RPC identifier space exhausted"))?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        let (response, receiver) = oneshot::channel();
        state.calls.insert(
            id,
            Call {
                caller,
                route: route.clone(),
                owner: None,
                deadline,
                payload,
                response: Some(response),
                retained_bytes,
            },
        );
        state.routes.get_mut(&route).unwrap().queue.push_back(id);
        state.retained_bytes += retained_bytes;
        state.accepted = state.accepted.saturating_add(1);
        state.notify_route(&route);
        Ok(Ticket {
            rpc: self.clone(),
            call_id: id,
            deadline,
            receiver: Some(receiver),
        })
    }

    fn try_take(&self, worker_id: u64) -> Result<Option<Job>, Error> {
        loop {
            let mut state = self.lock();
            let worker = state
                .workers
                .get(&worker_id)
                .ok_or_else(|| Error::new(UNAUTHORIZED, "TAKE requires a registered worker"))?;
            if worker.running >= worker.concurrency {
                return Ok(None);
            }
            let route_name = worker.route.clone();
            state.expire_queued(&route_name);
            let Some(id) = state
                .routes
                .get_mut(&route_name)
                .and_then(|route| route.queue.pop_front())
            else {
                return Ok(None);
            };
            let call = state
                .calls
                .get_mut(&id)
                .expect("queue points to a live call");
            let now = Instant::now();
            // Check again after queue cleanup: time may have crossed the deadline under load.
            if call.deadline <= now {
                state.expire(id);
                drop(state);
                continue;
            }
            call.owner = Some(worker_id);
            let job = Job {
                call_id: id,
                remaining_ms: call
                    .deadline
                    .duration_since(now)
                    .as_nanos()
                    .div_ceil(1_000_000)
                    .clamp(1, 30_000) as u32,
                payload: call.payload.clone(),
            };
            state.workers.get_mut(&worker_id).unwrap().running += 1;
            return Ok(Some(job));
        }
    }

    pub async fn take(
        self: &Arc<Self>,
        worker_id: u64,
        wait_ms: u32,
    ) -> Result<Option<Job>, Error> {
        let notify = {
            let mut state = self.lock();
            let worker = state
                .workers
                .get_mut(&worker_id)
                .ok_or_else(|| Error::new(UNAUTHORIZED, "TAKE requires a registered worker"))?;
            if worker.pending_takes >= worker.concurrency {
                return Err(Error::new(
                    OVERLOADED,
                    "worker TAKE waiter capacity reached",
                ));
            }
            worker.pending_takes += 1;
            worker.notify.clone()
        };
        let _guard = TakeGuard {
            rpc: self.clone(),
            worker_id,
        };
        let end = Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(job) = self.try_take(worker_id)? {
                return Ok(Some(job));
            }
            if timeout_at(end, notified).await.is_err() {
                return Ok(None);
            }
        }
    }

    pub fn complete(
        &self,
        worker_id: u64,
        id: u64,
        failed: bool,
        payload: Bytes,
    ) -> Result<(), Error> {
        let mut state = self.lock();
        if !state.workers.contains_key(&worker_id) {
            return Err(Error::new(
                UNAUTHORIZED,
                "COMPLETE requires a registered worker",
            ));
        }
        let call = state
            .calls
            .get(&id)
            .ok_or_else(|| Error::new(INVALID, "unknown or already completed call"))?;
        if call.owner != Some(worker_id) {
            return Err(Error::new(UNAUTHORIZED, "call belongs to another worker"));
        }
        let mut call = state.remove(id).unwrap();
        state.completed = state.completed.saturating_add(1);
        if let Some(response) = call.response.take() {
            let result = if call.deadline <= Instant::now() {
                state.timed_out = state.timed_out.saturating_add(1);
                Err(deadline_error())
            } else if failed {
                Err(Error::new(
                    HANDLER_ERROR,
                    String::from_utf8_lossy(&payload).into_owned(),
                ))
            } else {
                Ok(payload)
            };
            let _ = response.send(result);
        }
        Ok(())
    }

    fn expire(&self, id: u64) {
        self.lock().expire(id);
    }

    fn cancel(&self, id: u64) {
        let mut state = self.lock();
        if let Some(call) = state.calls.get_mut(&id) {
            call.response.take();
            if call.owner.is_none() {
                state.remove(id);
            }
        }
    }

    pub fn disconnect(&self, connection: u64) {
        let mut state = self.lock();
        state.connections.remove(&connection);
        let caller_ids = state
            .calls
            .iter()
            .filter(|(_, call)| call.caller == connection)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in caller_ids {
            let call = state.calls.get_mut(&id).unwrap();
            call.response.take();
            if call.owner.is_none() {
                state.remove(id);
            }
        }
        if let Some(worker) = state.workers.remove(&connection) {
            worker.notify.notify_waiters();
            let running = state
                .calls
                .iter()
                .filter(|(_, call)| call.owner == Some(connection))
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for id in running {
                if let Some(mut call) = state.remove(id) {
                    if let Some(response) = call.response.take() {
                        let _ = response.send(Err(Error::new(
                            SERVICE_UNAVAILABLE,
                            "worker disconnected; call will not be replayed",
                        )));
                    }
                }
            }
            let route = state.routes.get_mut(&worker.route).unwrap();
            route.workers.retain(|id| *id != connection);
            if route.workers.is_empty() {
                let route = state.routes.remove(&worker.route).unwrap();
                for id in route.queue {
                    if let Some(mut call) = state.remove(id) {
                        if let Some(response) = call.response.take() {
                            let _ = response.send(Err(Error::new(
                                SERVICE_UNAVAILABLE,
                                "last worker disconnected",
                            )));
                        }
                    }
                }
            } else {
                state.notify_route(&worker.route);
            }
        }
    }

    pub fn stats(&self) -> Value {
        let state = self.lock();
        let running = state
            .calls
            .values()
            .filter(|call| call.owner.is_some())
            .count();
        let mut routes = state.routes.iter().map(|(name, route)| {
            let workers = route.workers.iter().filter_map(|id| state.workers.get(id));
            let (concurrency, running) = workers.fold((0, 0), |(capacity, running), worker| (capacity + worker.concurrency, running + worker.running));
            json!({ "name": name, "workers": route.workers.len(), "concurrency": concurrency, "running": running, "queued": route.queue.len(), "queueLimit": route.queue_limit })
        }).collect::<Vec<_>>();
        routes.sort_unstable_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        json!({ "queued": state.calls.len() - running, "running": running, "retainedBytes": state.retained_bytes, "maxCalls": state.max_calls, "maxBytes": state.max_bytes, "accepted": state.accepted, "completed": state.completed, "rejected": state.rejected, "timedOut": state.timed_out, "routes": routes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> Bytes {
        Bytes::from_static(b"request")
    }
    fn register(rpc: &Rpc, id: u64, concurrency: usize, queue_limit: usize) {
        rpc.connected(id);
        rpc.connected(10);
        rpc.register(id, "users.get".into(), concurrency, queue_limit)
            .unwrap();
    }

    #[tokio::test]
    async fn queue_and_worker_limits_never_broadcast_or_dispatch_expired_calls() {
        let rpc = Rpc::new(100, 1 << 20);
        register(&rpc, 1, 1, 1);
        register(&rpc, 2, 1, 1);
        let first = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        assert_eq!(
            rpc.request(10, "users.get".into(), 1000, payload())
                .err()
                .unwrap()
                .code,
            OVERLOADED
        );
        let job = rpc.take(1, 10).await.unwrap().unwrap();
        assert!(rpc.take(2, 1).await.unwrap().is_none());
        let second = rpc.request(10, "users.get".into(), 5, payload()).unwrap();
        assert_eq!(second.wait().await.err().unwrap().code, DEADLINE_EXCEEDED);
        assert!(rpc.take(2, 1).await.unwrap().is_none());
        rpc.complete(1, job.call_id, false, Bytes::from_static(b"ok"))
            .unwrap();
        assert_eq!(first.wait().await.unwrap(), b"ok"[..]);
        assert_eq!(rpc.stats()["retainedBytes"], 0);
    }

    #[tokio::test]
    async fn running_timeouts_hold_capacity_until_actual_completion() {
        let rpc = Rpc::new(2, 1 << 20);
        register(&rpc, 1, 1, 4);
        let first = rpc.request(10, "users.get".into(), 5, payload()).unwrap();
        let job = rpc.take(1, 1).await.unwrap().unwrap();
        assert_eq!(first.wait().await.err().unwrap().code, DEADLINE_EXCEEDED);
        let second = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        assert!(rpc.take(1, 1).await.unwrap().is_none());
        assert_eq!(rpc.stats()["running"], 1);
        assert_eq!(rpc.stats()["queued"], 1);
        assert_eq!(
            rpc.request(10, "users.get".into(), 1000, payload())
                .err()
                .unwrap()
                .code,
            OVERLOADED
        );
        rpc.complete(1, job.call_id, false, Bytes::new()).unwrap();
        let next = rpc.take(1, 1).await.unwrap().unwrap();
        rpc.complete(1, next.call_id, false, payload()).unwrap();
        second.wait().await.unwrap();
        assert_eq!(rpc.stats()["running"], 0);
        assert_eq!(rpc.stats()["retainedBytes"], 0);
    }

    #[tokio::test]
    async fn cancellation_and_disconnect_clean_queued_calls_but_preserve_running_slots() {
        let rpc = Rpc::new(10, 1 << 20);
        register(&rpc, 1, 1, 4);
        let cancelled = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        drop(cancelled);
        assert_eq!(rpc.stats()["queued"], 0);
        let running = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        let job = rpc.take(1, 1).await.unwrap().unwrap();
        let queued = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        rpc.disconnect(10);
        assert_eq!(rpc.stats()["queued"], 0);
        assert_eq!(rpc.stats()["running"], 1);
        drop(running);
        drop(queued);
        rpc.complete(1, job.call_id, false, payload()).unwrap();
        assert_eq!(rpc.stats()["retainedBytes"], 0);
    }

    #[tokio::test]
    async fn worker_failure_never_replays_and_last_disconnect_fails_queue() {
        let rpc = Rpc::new(10, 1 << 20);
        register(&rpc, 1, 1, 4);
        let running = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        rpc.take(1, 1).await.unwrap().unwrap();
        let queued = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        rpc.disconnect(1);
        assert_eq!(
            running.wait().await.err().unwrap().code,
            SERVICE_UNAVAILABLE
        );
        assert_eq!(queued.wait().await.err().unwrap().code, SERVICE_UNAVAILABLE);
        assert_eq!(rpc.stats()["retainedBytes"], 0);
        assert_eq!(rpc.stats()["routes"].as_array().unwrap().len(), 0);
        register(&rpc, 2, 1, 4);
        assert!(rpc.take(2, 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn take_waits_for_work_and_owner_and_duplicate_completion_are_checked() {
        let rpc = Rpc::new(10, 1 << 20);
        register(&rpc, 1, 1, 4);
        register(&rpc, 2, 1, 4);
        let taking = {
            let rpc = rpc.clone();
            tokio::spawn(async move { rpc.take(1, 1000).await })
        };
        tokio::task::yield_now().await;
        let ticket = rpc
            .request(10, "users.get".into(), 1000, payload())
            .unwrap();
        let job = taking.await.unwrap().unwrap().unwrap();
        assert_eq!(
            rpc.complete(2, job.call_id, false, payload())
                .err()
                .unwrap()
                .code,
            UNAUTHORIZED
        );
        rpc.complete(1, job.call_id, true, Bytes::from_static(b"handler broke"))
            .unwrap();
        assert_eq!(ticket.wait().await.err().unwrap().code, HANDLER_ERROR);
        assert_eq!(
            rpc.complete(1, job.call_id, false, payload())
                .err()
                .unwrap()
                .code,
            INVALID
        );
    }

    #[tokio::test]
    async fn global_byte_budget_and_pending_take_bounds_are_enforced() {
        let rpc = Rpc::new(10, 1);
        register(&rpc, 1, 1, 4);
        assert_eq!(
            rpc.request(10, "users.get".into(), 1000, payload())
                .err()
                .unwrap()
                .code,
            OVERLOADED
        );
        assert_eq!(rpc.stats()["retainedBytes"], 0);
        assert_eq!(
            rpc.request(10, "missing".into(), 1000, payload())
                .err()
                .unwrap()
                .code,
            NO_SERVICE
        );
        assert!(rpc.register(1, "another".into(), 1, 1).is_err());
        assert!(rpc.register(2, "users.get".into(), 1, 3).is_err());
        let taking = {
            let rpc = rpc.clone();
            tokio::spawn(async move { rpc.take(1, 1000).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(rpc.take(1, 1).await.err().unwrap().code, OVERLOADED);
        taking.abort();
        let _ = taking.await;
        assert!(rpc.take(1, 1).await.unwrap().is_none());
    }

    #[test]
    fn disconnected_connections_cannot_readmit_work_or_recreate_worker_routes() {
        let rpc = Rpc::new(10, 1 << 20);
        register(&rpc, 1, 1, 4);
        rpc.disconnect(1);
        assert_eq!(
            rpc.register(1, "users.get".into(), 1, 4)
                .err()
                .unwrap()
                .code,
            SERVICE_UNAVAILABLE
        );
        rpc.disconnect(10);
        assert_eq!(
            rpc.request(10, "users.get".into(), 1000, payload())
                .err()
                .unwrap()
                .code,
            SERVICE_UNAVAILABLE
        );
        assert_eq!(rpc.stats()["retainedBytes"], 0);
        assert!(rpc.stats()["routes"].as_array().unwrap().is_empty());
    }
}
