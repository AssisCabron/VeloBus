# Próximas features para APIs e microsserviços

Estas são propostas para depois da v0.2, não funcionalidades já implementadas. A prioridade é melhorar proteção, diagnóstico e integração. Os mecanismos abaixo têm precedentes; a diferenciação precisa aparecer na facilidade de uso e em resultados medidos.

| Ordem | Feature | Exemplo de uso | Como comprovar valor |
| --- | --- | --- | --- |
| 1 | Métricas por fase e rastreamento distribuído | Ver quanto `orders.create` esperou na fila e quanto gastou no serviço | Separar espera/execução, correlacionar chamadas entre APIs e medir custo da instrumentação |
| 2 | Cotas e atendimento justo por aplicação e rota | Importações em lote não esgotam toda a capacidade disponível para checkout | Saturar uma rota e medir latência e rejeições da rota protegida |
| 3 | Encerramento gradual de workers | Publicar uma versão sem interromper chamadas saudáveis já iniciadas | Parar novas atribuições, concluir trabalho ativo e encerrar dentro de prazo máximo |
| 4 | Propagação de prazo e cancelamento individual | Um pedido com 500 ms restantes não inicia uma dependência com novo prazo de 5 s | Conferir prazos ao longo da cadeia e manter capacidade ocupada por handlers que ignoram cancelamento |
| 5 | Compartilhamento de consultas idênticas em andamento | Cem consultas simultâneas do mesmo produto podem compartilhar uma consulta ao banco | Contar execuções reais, respostas entregues e isolamento de permissões sob carga |
| 6 | SDK com contratos de rotas e adaptadores | Integrar `orders.create` tipado em Fastify, Express ou NestJS com poucas linhas | Exemplo instalável, checagem de tipos e mapeamento consistente dos erros para HTTP |
| 7 | Concorrência adaptativa com teto rígido | Reduzir admissão quando a dependência fica lenta e recuperar gradualmente | Comparar com limite fixo, incluindo oscilação, justiça, latência e perda de capacidade |
| 8 | Concluir uma chamada e pedir a próxima na mesma operação | Worker ocupado envia seu resultado e recebe o próximo trabalho usando menos mensagens de controle | Comparar CPU, bytes e p99 com `COMPLETE`/`TAKE` separados, sem enfraquecer prazos ou justiça |

## Contratos antes de otimizar

**Rastreamento:** propagar contexto compatível com [W3C Trace Context](https://www.w3.org/TR/trace-context/) e [OpenTelemetry](https://opentelemetry.io/docs/concepts/context-propagation/). Metadados precisam de limites de tamanho e validação. Não registrar payloads, tokens ou identificadores pessoais por padrão. Limitar cardinalidade de métricas; um identificador de pedido não deve virar uma série nova.

**Justiça:** o limite de fila por rota atual não reserva parcela do orçamento global. Projetar limites de chamadas e bytes por identidade autenticada e por rota, pesos e uso da capacidade ociosa. Reservar capacidade no broker não resolve um banco compartilhado saturado. Expor o motivo de rejeição para distinguir quota, fila e orçamento global.

**Drain:** `service.close()` atual desconecta e pode falhar chamadas ativas. Uma futura operação de drain deve parar novas atribuições, manter as execuções atuais e só fechar quando terminarem ou atingir o limite de encerramento. Definir comportamento da fila quando sair o último worker.

**Prazos:** seguir o princípio de descontar o tempo já gasto ao encaminhar uma chamada, descrito também pelo [gRPC](https://grpc.io/docs/guides/deadlines/). Cancelamento é cooperativo; não desfaz transações nem prova que um efeito externo não ocorreu. Uma chamada expirada não deve ser repetida automaticamente.

**Consultas compartilhadas:** adesão explícita por rota de leitura; não inferir equivalência apenas pelo payload. A chave precisa incluir versão da operação e contexto de autorização/tenant. Cada solicitante tem seu prazo, e espera, bytes e tamanho da resposta continuam limitados. Cancelar um solicitante não cancela automaticamente a consulta dos demais. Cache com TTL é outro recurso, com regras próprias de invalidação.

**Integração:** tipos TypeScript ajudam quem usa o SDK, mas não validam dados recebidos em tempo de execução. Os adaptadores devem preservar prazos, erros estruturados, cancelamento, ciclo de vida e limites HTTP de entrada.

**Adaptação:** começar com limites fixos e métricas confiáveis. Controlar admissão gradualmente dentro de pisos/tetos definidos; reduzir uma configuração não interrompe trabalho já iniciado. Evitar que o controlador confunda saturação do gerador ou da rede com saturação do serviço.

**Protocolo:** uma operação futura `COMPLETE_AND_TAKE` poderia reduzir mensagens de controle no caminho dos workers. A resposta do solicitante não deve esperar a chegada de outro trabalho. Preservar autenticação, confirmação inequívoca da conclusão, concorrência, justiça da fila e comportamento durante desconexão; não repetir uma conclusão automaticamente após resposta perdida. É uma hipótese de otimização, não um ganho medido.

## Após esta rodada

Minha recomendação de implementação é **métricas por fase → cotas justas → drain → integração tipada**, usando os benchmarks como referência de regressão. Consultas compartilhadas e concorrência adaptativa ficam como experimentos isolados e mensuráveis.

Antes de uso entre redes ou times distintos, também são necessários identidade por serviço, autorização por rota e transporte protegido. Particionamento, replicação e alta disponibilidade exigem uma etapa própria; aumentar workers de uma rota não elimina o broker único como ponto de falha.
