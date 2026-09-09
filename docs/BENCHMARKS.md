# Primeiras medições locais

Estes números são históricos do núcleo de **eventos v0.1**. Não medem a nova comunicação request/reply de APIs. O ensaio de controle de sobrecarga RPC fica em [RPC-VALIDATION.md](RPC-VALIDATION.md).

Medições de desenvolvimento em **9 de setembro de 2026**, no Mac Apple M2 ARM64 com 8 GB de RAM, Node v26.7.0 e Rust 1.97.1. Binário em perfil `release`: **951.424 bytes** (aproximadamente 929 KiB). **Nenhum teste no Raspberry Pi ou comparação com outros brokers foi realizado.**

## Carga medida

Cada execução publica **100.000 eventos de 256 bytes**, em lotes de até **128**, com um produtor e 100 chaves repetidas. Depois da publicação, um consumidor lê todo o histórico em `all`; em seguida, outro percurso lê em `latest`. Cliente e servidor rodam na mesma máquina, em TCP loopback, sem TLS e sem replicação. Não há aquecimento nem simultaneidade entre as fases. São execuções curtas, não um ensaio prolongado de capacidade.

| Medida | Memória, sem persistência | Disco, sincronização antes do ACK |
|---|---:|---:|
| Publicações confirmadas | 100.000 | 100.000 |
| Tempo da fase de publicação | 158,84 ms | 2.990,59 ms |
| Publicações confirmadas por segundo | 629.559 | 33.438 |
| Latência p50 de ACK **por lote** | 0,126 ms | 3,009 ms |
| Latência p99 de ACK **por lote** | 1,200 ms | 9,218 ms |
| Maior latência de ACK por lote | 13,246 ms | 219,197 ms |
| Eventos entregues em `all` | 100.000 | 100.000 |
| Eventos entregues em `latest` | 2.500 | 2.500 |
| Maior RSS amostrado do broker | 51,95 MiB | 50,31 MiB |

As taxas representam trabalho confirmado do cliente ao broker. Não são latências fim a fim de processamento da aplicação e não devem ser extrapoladas para Raspberry Pi, rede física, múltiplos consumidores ou produção. RSS foi amostrado a cada 50 ms; picos entre amostras podem não ter sido vistos. As condições de persistência dependem do sistema operacional e do armazenamento usado.

## Efeito do modo `latest`

Nesta carga sintética com 100 chaves repetidas, `latest` entregou 2.500 eventos em 25 janelas, reduzindo o conteúdo entregue de **25.600.000 para 640.000 bytes**: **97,5% menos entregas/conteúdo** nessa fase. Isso não é redução equivalente do custo total: a entrada, o histórico em memória e o WAL continuam contendo os 100.000 eventos originais. Todos eles permaneceram disponíveis para `all`.

Essa economia somente é válida para consumidores que aceitam omitir estados intermediários. A combinação é limitada à janela examinada; não é uma deduplicação global do histórico.

## Reproduzir

```sh
npm run setup
npm run build
npm run benchmark -- --messages 100000 --payload 256 --batch 128 --mode memory --output docs/benchmarks/local-memory.json
npm run benchmark -- --messages 100000 --payload 256 --batch 128 --mode disk --output docs/benchmarks/local-disk.json
```

Resultados brutos: [memória](benchmarks/local-memory.json) e [disco](benchmarks/local-disk.json).

## O que falta medir

Raspberry Pi 5 real; carga sustentada; produtores e consumidores concorrentes; mensagens e chaves variadas; latência fim a fim; saturação da rede; recuperação com log próximo ao limite; energia; e comparação com NATS/RabbitMQ sob os mesmos contratos. O histórico atual é limitado e retido em memória: prolongar ingestão indefinidamente requer a próxima etapa de armazenamento segmentado e retenção explícita.
