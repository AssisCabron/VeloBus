# Nodara

Intermediário experimental para APIs e microsserviços: **broker Rust + biblioteca Node.js/TypeScript**, com request/reply, limites de carga e suporte a múltiplos brokers. Nome anterior: VeloBus; o repositório Git mantém seu endereço original.

A v0.3 distribui chamadas entre até oito brokers independentes. Se um nó cair, novas chamadas podem seguir pelos restantes; o SDK reconecta e registra seus workers quando ele retorna. **Chamadas em andamento e filas não são replicadas.** Uma resposta perdida pode deixar o resultado incerto; operações não são repetidas automaticamente nessas condições.

## Instalação

```sh
npm install nodara
```

Node.js 20+, ESM/CommonJS, tipos TypeScript e nenhuma dependência de execução. O pacote npm contém o cliente; o broker Rust roda separadamente. Licença MIT.

Para compilar o broker a partir deste repositório:

```sh
npm run setup
npm run build
./target/release/nodara --memory --listen 127.0.0.1:7447
```

Em outros terminais/hosts, inicie mais brokers, por exemplo nas portas 7448 e 7449. Cada broker em modo disco exige diretório de dados próprio. Fora de loopback, configure `NODARA_TOKEN` e proteja o transporte externamente; não há TLS nativo.

## Worker com múltiplos brokers

```js
import { connectCluster } from 'nodara';

const bus = await connectCluster({
  brokers: [
    { id: 'a', host: '127.0.0.1', port: 7447 },
    { id: 'b', host: '127.0.0.1', port: 7448 },
    { id: 'c', host: '127.0.0.1', port: 7449 },
  ],
  // token: process.env.NODARA_TOKEN,
});

const service = await bus.handleJSON('users.get', async ({ id }, context) => {
  // Integre seu backend e encaminhe context.signal a operações canceláveis.
  return { id, name: 'Ana' };
}, { concurrency: 8, queueLimit: 128 });

console.log(service.nodes());
// No encerramento: await service.close(); await bus.close();
```

`concurrency: 8` limita os handlers desse serviço a oito execuções simultâneas **nesse processo**, somando todos os brokers e preservando vagas de handlers antigos durante reconexões. `queueLimit` é por rota em cada broker. A espera local pelos handlers também é limitada e consome o prazo da chamada. Veja o [contrato completo de capacidade](docs/CLUSTER.md).

## Chamada a partir de outra API

Crie um `connectCluster()` com a mesma lista e reutilize-o:

```js
const user = await bus.requestJSON('users.get', { id: 'U-1024' }, { timeoutMs: 1500 });
console.log(user);
console.log(bus.nodes());
```

A aplicação usa `await`; o SDK não bloqueia o event loop. Cada tentativa aceita é entregue a um worker. O balanceamento usa pendências locais e alterna empates; não mede a CPU global de todos os brokers. Uma rejeição explícita antes do despacho pode ser tentada em outro nó dentro do mesmo prazo.

Para um único broker, `connect({ host, port, token })` continua disponível, incluindo eventos. CommonJS: `const { connectCluster } = require('nodara')`.

## Falhas e garantias

| Resultado | Comportamento |
| --- | --- |
| Nó desconectado | Retirado da seleção; reconexão com recuo limitado |
| `NO_SERVICE` / `OVERLOADED` antes do despacho | Pode tentar outro nó, no máximo uma vez por nó e dentro do prazo original |
| `AmbiguousResultError` | A chamada pode ter executado; não é repetida automaticamente |
| Prazo esgotado ou erro do handler | Devolvido à API, sem replay em outro broker |
| Todos os nós indisponíveis | Falha explícita, sem aceitar trabalho em fila ilimitada |
| Broker recuperado | Conexão e registros de workers restaurados; chamadas antigas não são restauradas |

Timeout/cancelamento não desfaz efeitos externos. Pedidos e pagamentos precisam de idempotência e reconciliação duráveis na aplicação. Nós no mesmo host compartilham a falha desse host. O pool não oferece consenso, replicação, descoberta automática, cotas globais entre processos ou certificação de alta disponibilidade.

## Eventos

`connect()` mantém `publish`, `publishBatch`, `fetch` e `subscribe`. Cada nó tem seu próprio histórico, sequência e WAL. O pool não mistura eventos entre nós. O ACK de evento no modo disco confirma sincronização do WAL local, não processamento pelo consumidor. RPC continua volátil mesmo quando o WAL está habilitado.

`latest` combina estados da mesma chave durante uma busca limitada e só é adequado quando estados intermediários podem ser omitidos. Não combina chamadas RPC.

## Exemplos, testes e documentação

```sh
npm test
npm run build
npm run test:package
npm run test:benchmark
npm run example:cluster
npm run example:http
```

`example:cluster` inicia três brokers locais, encerra um e demonstra atendimento pelos restantes. Os testes de cluster exercitam queda durante execução, ausência de replay, limite compartilhado, autenticação e recuperação.

O [benchmark histórico v0.2](docs/RPC-BENCHMARKS.md) mediu 48,3 mil respostas/s de mediana com payload de 256 B e 32 chamadas simultâneas no Apple M2. **Ele não mede o novo pool, Raspberry Pi ou tráfego de produção.** Não há comparação de superioridade sobre outros brokers.

- [Múltiplos brokers e garantias](docs/CLUSTER.md)
- [Guia completo para LLMs](LLM.txt) e [índice llms.txt](llms.txt)
- [SDK](packages/client/README.md), [protocolo RPC](docs/RPC-PROTOCOL.md) e [eventos](docs/PROTOCOL.md)
- [Propostas de features](docs/FEATURES.md) e [validação](docs/VALIDATION.md)

Migração do nome anterior: pacote `nodara`, binário `nodara`, variáveis `NODARA_TOKEN`, `NODARA_HOST`, `NODARA_PORT`, `NODARA_BIN` e `NODARA_WORK_DIR`. O diretório padrão passa a `./data/nodara`; para usar um WAL existente, informe seu diretório explicitamente com `--data-dir`.
