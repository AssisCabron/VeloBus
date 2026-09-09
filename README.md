# VeloBus

Intermediário experimental entre **APIs e microsserviços**, com núcleo Rust e cliente TypeScript/Node.js. Uma API envia uma requisição para uma rota, o VeloBus a entrega a um worker disponível e devolve a resposta ao solicitante. Filas limitadas, concorrência declarada e prazos impedem que a espera cresça sem controle dentro do broker.

O foco da v0.2 é **request/reply**. A aplicação faz `await` até receber a resposta ou um erro; a comunicação usa I/O assíncrona e não bloqueia o event loop Node.js. Cada chamada é entregue a no máximo um worker. Não é broadcast.

Versão experimental de um único nó. Raspberry Pi 5 com Linux de 64 bits é um alvo de avaliação; desempenho nesse hardware ainda precisa ser medido. O nome é provisório e `@velobus/client` ainda não foi publicado no registro npm.

## Começar

Requisitos: Rust/Cargo estável e Node.js 20 ou superior.

```sh
npm run setup
npm run build
npm start
```

O broker escuta em `127.0.0.1:7447`. Para desenvolvimento sem o WAL de eventos, use `npm run dev`. **RPC é volátil nos dois modos:** habilitar o WAL não torna chamadas request/reply persistentes.

Com o broker rodando, em outro terminal:

```sh
npm run example
# Demonstra uma API HTTP e duas instâncias do serviço users.get:
npm run example:http
# Em um terceiro terminal:
curl http://127.0.0.1:8080/users/42
```

O exemplo HTTP limita chamadas ao backend e traduz sobrecarga em HTTP 503 e prazo esgotado em HTTP 504. Os dois workers do exemplo compartilham um processo Node para facilitar a execução; em produção, podem estar em processos/servidores separados. Para adaptar uma API HTTP existente, um handler pode chamar `fetch(url, { signal: context.signal })` dentro da concorrência declarada.

O cliente é um pacote npm comum, com ESM, CommonJS e tipos TypeScript, sem dependências de execução ou addon nativo. O servidor Rust é executado separadamente. Enquanto o pacote não estiver publicado:

```sh
npm install /caminho/para/velobus/packages/client
```

## Registrar um microsserviço

O SDK usa uma conexão autenticada dedicada para cada worker registrado.

```js
import { connect } from '@velobus/client';

const bus = await connect();
const service = await bus.handleJSON(
  'users.get',
  async ({ id }, context) => {
    // Exemplo: substitua pelo acesso ao seu banco ou serviço.
    // Encaminhe context.signal a operações que aceitam cancelamento.
    return { id, name: 'Ana' };
  },
  { concurrency: 8, queueLimit: 128 },
);

// No encerramento: await service.close(); await bus.close();
```

Outros processos podem registrar a mesma rota. Cada worker respeita sua concorrência; todos devem usar o mesmo `queueLimit`, que limita a fila compartilhada de espera da rota. O padrão é 8 execuções simultâneas por worker; o SDK limita a configuração a 32.

## Chamar a partir de uma API

```js
import { connect } from '@velobus/client';

const bus = await connect();
const user = await bus.requestJSON(
  'users.get',
  { id: 'U-1024' },
  { timeoutMs: 2000 },
);

console.log(user);
// Reutilize bus; feche a conexão no encerramento da aplicação.
```

Também há `request` e `handle` para payloads binários ou texto. Navegadores acessam sua API HTTP; o SDK TCP roda no backend. O broker não transforma endpoints HTTP existentes em workers automaticamente: a aplicação registra handlers e faz chamadas pelo cliente.

## Sobrecarga, prazo e falhas

- **Fila cheia ou orçamento global esgotado:** `OVERLOADED` rapidamente; uma API HTTP pode responder 503.
- **Nenhum worker para a rota:** `NO_SERVICE`. Se o último worker desconectar, a fila pendente falha.
- **Prazo esgotado:** `DEADLINE_EXCEEDED`, incluindo espera na fila e execução; uma API HTTP pode responder 504. Um pedido expirado na fila não deve começar.
- **Handler já iniciado:** timeout não desfaz efeitos externos. O SDK sinaliza cancelamento cooperativo e só libera a vaga quando o handler termina ou o worker desconecta. Código que ignora o sinal pode continuar.
- **Worker desconectado:** chamadas ativas recebem `SERVICE_UNAVAILABLE`, sem repetição automática. Desconectar não prova que efeitos externos tenham sido interrompidos.

O prazo padrão é 5 segundos, configurável de 1 ms a 30 segundos. O timeout de transporte inclui uma margem além do prazo RPC. Erros normais de serviço preservam a conexão do chamador; falhas de transporte podem deixar o resultado de uma operação incerto. Use identificadores de negócio e idempotência quando a aplicação decidir repetir chamadas.

## Limites e operação

```sh
./target/release/velobus \
  --listen 127.0.0.1:7447 \
  --max-connections 64 \
  --max-rpc-calls 1024 \
  --max-rpc-bytes 16777216
```

O orçamento RPC inclui chamadas em espera e execução. Trabalho já iniciado continua ocupando capacidade até terminar ou perder seu worker, mesmo se o solicitante desistir. `stats().rpc` mostra filas, execuções e contadores por rota. Os orçamentos internos de bytes não limitam todo o RSS do processo.

Use `VELOBUS_TOKEN` para autenticação; ele é obrigatório fora de loopback. TCP com token não oferece criptografia, portanto acesso remoto requer transporte protegido externamente. Não há TLS embutido, alta disponibilidade, replicação, replay automático, circuit breaker, limite por segundo ou escalonamento automático nesta versão.

## Eventos como recurso complementar

`publish`, `publishBatch`, `fetch` e `subscribe` continuam disponíveis. Em disco, o ACK de publicação significa sincronização do lote no WAL local, sujeito ao sistema de arquivos e ao armazenamento. Não confirma processamento por consumidores e não se aplica a chamadas RPC.

O histórico de eventos é limitado, mantido em memória e reconstruído do WAL. Capacidade esgotada rejeita publicações sem expulsão silenciosa. Não há retenção automática nem consumidores duráveis no servidor. Checkpoints pertencem ao mesmo histórico e assinatura; recriar o WAL ou reiniciar o modo volátil invalida os anteriores.

`latest` combina estados da mesma combinação de tópico e chave dentro de uma busca limitada, preservando o histórico. Só use quando a assinatura aceita omitir estados intermediários. **Chamadas RPC nunca são combinadas por `latest`.**

## Validar e medir

```sh
npm test
npm run build
npm run test:package
npm run pack:client
npm run load:rpc
npm run test:benchmark
npm run benchmark:rpc -- --output docs/benchmarks/local-rpc.json
```

Os scripts usam `work/` para temporários; `VELOBUS_WORK_DIR` permite outro diretório. O pacote instalável é gerado em `artifacts/`.

O [benchmark RPC](docs/RPC-BENCHMARKS.md) mede respostas completas, latências, rejeições e recursos com processos separados, aquecimento e repetições. O comando completo demora alguns minutos e usa o binário release. A checagem `test:benchmark` é curta e valida o instrumento, sem exigir uma meta de desempenho em CI. Os benchmarks de eventos em [BENCHMARKS.md](docs/BENCHMARKS.md) não demonstram capacidade request/reply. Não há alegação de vantagem universal sobre outros intermediários.

Referência local no Apple M2: com payload de 256 B e 32 chamadas simultâneas, a mediana de três execuções foi **48,3 mil respostas/s**, com **p99 mediano de 1,52 ms**. Cada execução teve 2 s de aquecimento e 10 s de admissão. O relatório inclui variação, rejeições, pausas do gerador e CPU/memória de cada processo. Esses números não foram medidos no Raspberry Pi.

As [novas propostas de features](docs/FEATURES.md) priorizam métricas por fase, cotas justas, encerramento gradual e integração tipada.

Detalhes: [request/reply](docs/RPC-PROTOCOL.md), [eventos](docs/PROTOCOL.md), [arquitetura](docs/ARCHITECTURE.md), [roteiro](docs/ROADMAP.md), [cliente](packages/client/README.md).
