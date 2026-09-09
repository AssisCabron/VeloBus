# Nodara: múltiplos brokers

A v0.3 adiciona distribuição de request/reply no SDK Node.js. O cliente mantém conexões com até oito brokers independentes, escolhe o nó com menos chamadas locais pendentes e alterna os empates. Cada broker continua executando um processo Rust próprio. Não há encaminhamento entre brokers, descoberta automática ou consenso distribuído.

## Configuração

```js
import { connectCluster } from 'nodara';

const bus = await connectCluster({
  brokers: [
    { id: 'a', host: '127.0.0.1', port: 7447 },
    { id: 'b', host: '127.0.0.1', port: 7448 },
    { id: 'c', host: '127.0.0.1', port: 7449 },
  ],
  // token: process.env.NODARA_TOKEN,
  timeoutMs: 1000,
  healthIntervalMs: 500,
  reconnectMinMs: 100,
  reconnectMaxMs: 5000,
  maxPendingRequests: 256,
});

const service = await bus.handleJSON('inventory.get', async ({ sku }, context) => {
  return { sku, available: true }; // Substitua pelo seu backend; propague context.signal.
}, { concurrency: 8, queueLimit: 128 });

console.log(await bus.requestJSON('inventory.get', { sku: 'A-42' }, { timeoutMs: 1500 }));
console.log(bus.nodes());
console.log(service.nodes());
await service.close();
await bus.close();
```

APIs chamadoras e workers podem usar processos diferentes, com a mesma lista de brokers. Um token pode ser comum ao pool ou informado por endpoint. Tokens não aparecem em `nodes()`. Fora de loopback, os brokers exigem `NODARA_TOKEN`; não há TLS nativo, então o transporte remoto precisa ser protegido externamente.

Execute três brokers com portas/diretórios separados. Para RPC volátil local:

```sh
./target/release/nodara --memory --listen 127.0.0.1:7447
./target/release/nodara --memory --listen 127.0.0.1:7448
./target/release/nodara --memory --listen 127.0.0.1:7449
```

Cada comando roda em um terminal/processo. Para demonstrar automaticamente três processos e derrubar um deles: `npm run example:cluster`, depois de `npm run build` e `cargo build`. Em produção, nós no mesmo host compartilham a falha desse host; a lista de endpoints não provisiona máquinas ou containers.

## Admissão, balanceamento e reconexão

- O número de pendências usado no balanceamento é local ao cliente, não a carga global, CPU ou fila de todos os clientes. Empates giram entre nós.
- No máximo 32 operações de transporte por conexão. Healthchecks contam nesse limite; o limite agregado do pool é configurável até 256 chamadas do usuário. Não há fila ilimitada no cliente.
- A inicialização exige pelo menos um broker conectado e autenticado. Nós indisponíveis são reconectados com recuo exponencial limitado e jitter. Se nenhum conectar, a inicialização falha e limpa recursos.
- Quedas detectadas removem o nó da seleção. Novas chamadas usam os restantes. A primeira chamada durante uma queda ainda não detectada pode falhar com resultado incerto.
- Quando o nó volta, o pool autentica novamente e registra automaticamente seus serviços ativos. `service.nodes()` informa em quais nós o registro está disponível. Reconexão não restaura chamadas antigas.
- `NO_SERVICE` (8), `OVERLOADED` (9) e rejeição local antes de envio permitem tentar outro broker. Cada nó é tentado no máximo uma vez por chamada, descontando o tempo já gasto do prazo original. Não há tempestade de retries em loop.

## Limite real de execução dos workers

Em `connectCluster().handle()`, **`concurrency` é o limite de handlers ativos para aquele serviço naquele processo Node, compartilhado entre os brokers**. Registrar oito vagas em três brokers não permite executar 24 handlers simultâneos nesse processo.

Cada registro pode receber até `concurrency` atribuições do seu broker, mas um gate local compartilhado admite o handler real. Por isso, pode haver até `nós × concurrency` atribuições pendentes de admissão/execução no SDK, com tamanho de payload limitado pelo protocolo. A espera local consome o prazo RPC; pedidos cujo sinal já foi cancelado não entram no handler. Esse buffer é limitado e não deve ser confundido com a fila de espera do broker. O broker mostra atribuições ao worker como `running`, mesmo quando aguardam o gate local.

O gate sobrevive às reconexões. Se um handler de um broker que caiu ignorar cancelamento, ele continua ocupando sua vaga até terminar. Isso protege a dependência real contra multiplicação de trabalho durante failover. Um handler que nunca termina pode bloquear capacidade indefinidamente; a aplicação deve cooperar com `context.signal` e limitar suas operações externas.

`queueLimit` continua sendo **por rota, por broker**. Os limites de chamadas/bytes dos brokers também são independentes. Adicionar nós aumenta a capacidade total de filas e pode aumentar o uso agregado de memória. Limites globais entre vários processos ou máquinas e cotas por tenant não estão implementados.

## O que uma queda preserva

| Situação | Resultado |
| --- | --- |
| Broker já identificado como indisponível | Novas chamadas são distribuídas pelos nós conectados |
| Chamada enviada sem resposta conclusiva quando a conexão falha | `AmbiguousResultError`, com `brokerId` e `outcome: 'unknown'`; sem replay automático |
| Pedido rejeitado explicitamente antes do despacho | Pode tentar outro nó dentro do mesmo prazo |
| Erro do handler ou prazo RPC esgotado | Erro devolvido ao chamador; não é repetido em outro nó |
| Todos os brokers indisponíveis | `ClusterUnavailableError`; não cria uma fila invisível de trabalho aceito |
| Worker em execução perde seu broker | Sinal de cancelamento cooperativo; efeitos externos podem continuar |

**RPC e filas não são replicados.** A queda de um nó pode perder as chamadas que estavam naquele nó. O benefício desta versão é manter o atendimento pelos outros nós e reconstruir registros; não garante zero perda ou execução exatamente uma vez. Para pagamentos/pedidos, mantenha identidade de negócio, idempotência e reconciliação duráveis na aplicação antes de decidir repetir resultados incertos.

O pool deliberadamente não expõe `publish`, `fetch` ou `subscribe`: cada broker tem seu próprio histórico, sequência e WAL. Distribuir essas operações sem um protocolo de replicação daria uma falsa impressão de histórico único. Use `connect()` para eventos em um nó identificado. Replicação durável/quórum permanece uma etapa separada.

## Validação

`npm run test:integration` inclui testes reais com dois brokers: balanceamento, limite de handlers compartilhado, SIGKILL, reconexão na mesma porta, registro restaurado, rejeição segura, resultado incerto sem replay, handler antigo que ignora cancelamento, autenticação por nó e encerramento. Os benchmarks históricos de v0.2 continuam sendo medições de um broker e não medem o desempenho deste pool.
