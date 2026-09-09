> Atualização v0.3: o pool de brokers independentes, balanceamento local e reconexão foram implementados no SDK Nodara. Veja [CLUSTER.md](CLUSTER.md). Replicação e consenso continuam fora do escopo implementado; as propostas abaixo seguem como roteiro.

# Roteiro técnico

O foco é intermediar chamadas entre APIs e microsserviços com resposta aguardada e capacidade controlada. As etapas abaixo são propostas, não recursos disponíveis nem promessas de prazo ou desempenho. A v0.2 continua experimental, em um único nó, com RPC volátil e sem replay automático.

Veja também o [backlog priorizado de features](FEATURES.md) e o [benchmark de request/reply](RPC-BENCHMARKS.md).

## 1. Validar proteção sob sobrecarga

Demonstrar filas e concorrência limitadas por rota, limites globais e erro rápido sem capacidade. Exercitar serviços lentos, handlers com erro, múltiplos workers, chamadas simultâneas e consumidores de resposta lentos.

Verificar que pedidos expirados na fila não executam, handlers tardios mantêm vagas e timeouts, desistências e desconexões limpam recursos. Medir o retorno à ocupação esperada após cada cenário. Uma fila curta com trabalho oculto ainda em execução não caracteriza proteção eficaz.

## 2. Medir no Raspberry Pi 5

Rodar em Linux de 64 bits no hardware real. Registrar RAM, refrigeração, temperatura, alimentação, sistema operacional e compilador. Separar custo do broker, cliente e serviço atendido.

Variar payload, rotas, workers, concorrência, duração dos handlers e taxa de chegada. Medir respostas, rejeições, prazos excedidos, latências p50/p95/p99 de ponta a ponta, tempo em fila, CPU, RSS e energia. Comparações exigem mesmas garantias e comportamento de sobrecarga; throughput de eventos persistidos não mede RPC.

Identificar gargalos antes de otimizar: rede, cópias, alocações, contenção ou serviço. Não assumir uma meta de desempenho como resultado.

## 3. Melhorar operação e integração

Avaliar métricas por rota, rastreamento, configuração operacional e encerramento que drene trabalho com prazo máximo. Definir entrada e saída de workers sem ultrapassar capacidade real.

Rate limit, circuit breaker, integração HTTP e balanceamento adaptativo exigem contratos e testes próprios. São possibilidades futuras; filas limitadas e concorrência fixa não equivalem a esses recursos.

## 4. Particionar e replicar com garantias explícitas

Avaliar distribuição de rotas após medir o nó único. Definir descoberta, propriedade, rebalanceamento e destino de chamadas em andamento. Replicar o registro de workers não torna pedidos em execução recuperáveis.

Antes de afirmar alta disponibilidade, definir aceitação e resposta durante falhas, partições de rede e resultados desconhecidos. Replay de RPC com efeitos externos exige idempotência ou contrato transacional; não deve surgir como repetição transparente.

Eventos usam sequência global. Particioná-los também exige versionar ordenação e cursores, sem aparentar uma ordem global que a implementação não garanta.

## 5. Evoluir o histórico complementar

Para eventos, projetar WAL segmentado, índices e leitura em disco para desacoplar histórico da RAM. Definir exportação, rotação e sinalização de cursores expirados antes de remover dados automaticamente.

Introduzir geração persistente e checkpoints vinculados à assinatura antes de consumidores duráveis. Compactação física exige contrato próprio porque `latest` preserva os originais. Essas melhorias não tornam RPC persistente por consequência.

## Critério comum

Cada etapa precisa de contratos verificáveis e resultados reproduzíveis. Rust, execução em um Pi ou um pico de throughput não demonstram vantagem universal. A proposta deve ser sustentada por custo, latência e estabilidade sob uma carga definida, preservando o comportamento exigido pelas aplicações.
