# Validação de comunicação entre APIs — v0.2

Executada em 9 de setembro de 2026 no Mac Apple M2, com broker Rust e cliente Node v26.7.0 por TCP loopback. O objetivo deste ensaio é verificar **controle de sobrecarga e encaminhamento exclusivo**, não estimar capacidade máxima ou comparar com outros produtos.

## Rajada de chamadas

Configuração: 64 chamadas simultâneas, dois clientes chamadores, duas instâncias registradas em `users.get`, concorrência 2 por instância e fila compartilhada de 8. Cada handler simula 50 ms de trabalho e cada chamada tem deadline de 2 segundos.

| Resultado observado | Quantidade |
|---|---:|
| Chamadas concluídas | 12 |
| Chamadas rejeitadas com OVERLOADED | 52 |
| Outros erros | 0 |
| Pico de handlers executando ao mesmo tempo | 4 |
| Execuções de handlers | 12 |
| Chamadas em fila ou execução ao final | 0 |
| Bytes RPC contabilizados ao final | 0 |

O pico respeitou a capacidade declarada. O excedente recebeu erro, sem encaminhamento para todos os serviços, execução duplicada ou fila ilimitada. A quantidade exata admitida depende do agendamento: este resultado não é uma garantia de aceitar sempre 12 chamadas em qualquer rajada.

Reproduza após compilar:

```sh
npm run build
VELOBUS_BIN="$PWD/target/release/velobus" npm run load:rpc -- docs/benchmarks/local-rpc-overload.json
```

Resultado bruto: [local-rpc-overload.json](benchmarks/local-rpc-overload.json).

## Casos de integração

Os testes por TCP real cobrem retorno JSON/binário, erros de negócio, serviço ausente, duas réplicas sem broadcast, chamada lenta junto de chamada rápida, concorrência e fila limitadas, deadline antes do despacho, deadline com handler ainda em execução, cancelamento de fila após desconexão do chamador, falha de worker sem reenvio, limites globais de chamadas/bytes, discordância de configuração entre réplicas e 32 slots simultâneos no mesmo worker.

O exemplo HTTP real foi exercitado com `GET /users/42`: retorna 200 na operação normal, 503 quando não há serviço/capacidade e 504 quando o prazo termina. A rajada HTTP também respeitou a concorrência declarada do backend.

## Limites da evidência

O chamador aguarda uma resposta com `await`; o event loop permanece assíncrono. Deadline não desfaz uma operação externa já iniciada. Um handler ainda executando conserva seu slot até terminar ou desconectar, mesmo que o chamador já tenha recebido timeout.

O ensaio é curto e local. Não prova desempenho no Raspberry Pi, capacidade sustentada, recuperação entre nós, alta disponibilidade ou proteção absoluta contra indisponibilidade. As taxas históricas do núcleo de eventos v0.1 não são taxas de RPC.
