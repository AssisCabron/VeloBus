# Benchmark de request/reply

O benchmark mede chamadas completas **cliente Node → broker Rust → worker Node → broker → cliente**, verificando os bytes de cada resposta. A capacidade observada pertence a esse conjunto e à carga descrita; não é um número isolado do núcleo Rust.

## Reproduzir

```sh
npm run setup
npm run build
npm run test:benchmark
npm run benchmark:rpc -- --output docs/benchmarks/local-rpc.json
```

Requisitos adicionais: `git` e `ps` de Linux/macOS. O script inicia e encerra seus próprios processos em portas locais efêmeras. Usa `target/release/velobus`; `VELOBUS_BIN` permite escolher outro executável. Não conecta a serviços externos.

Por padrão, são **2 s de aquecimento + 10 s de admissão por cenário, três repetições**, com uma instância nova do broker e dos workers a cada execução. A ordem dos cenários gira a cada repetição. Exemplos:

```sh
npm run benchmark:rpc -- --scenarios echo-c1,echo-c32 --duration 30 --warmup 5 --repetitions 3 --output work/rpc.json
npm run benchmark:rpc -- --scenarios slow-overload,slow-scale-out --duration 30 --output work/overload.json
```

`test:benchmark` verifica o instrumento e faz duas execuções curtas. Não usa metas de throughput ou p99 como condição de aprovação em CI. Essas execuções não são resultados de desempenho.

## Cargas

| Cenário | Modelo de chegada | Payload por sentido | Workers × vagas | Chamadas simultâneas máximas no cliente | Fila compartilhada |
| --- | --- | --- | --- | --- | --- |
| echo-c1 | Próximo pedido depois da resposta | 256 B | 1 × 1 | 1 | 128 |
| echo-c32 | Próximo pedido depois da resposta | 256 B | 1 × 32 | 32 | 128 |
| echo-c128 | Próximo pedido depois da resposta | 256 B | 4 × 32 | 128 | 256 |
| echo-64k | Próximo pedido depois da resposta | 64 KiB | 2 × 16 | 32 | 128 |
| slow-normal | 800 chegadas/s programadas | 256 B | 2 × 8 | 128 | 32 |
| slow-overload | 4.000 chegadas/s programadas | 256 B | 2 × 8 | 128 | 32 |
| slow-scale-out | 4.000 chegadas/s programadas | 256 B | 4 × 8 | 128 | 32 |

Nos cenários `echo`, o handler devolve o payload imediatamente. Nos cenários `slow`, espera um timer de 10 ms com cancelamento cooperativo e então responde: simula espera por I/O, não trabalho de CPU ou banco real. O prazo RPC é 1 s. Com 16 vagas e 10 ms por atendimento, o teto ideal do serviço simulado seria 1.600 execuções/s antes dos demais custos; com 32 vagas, 3.200/s.

## Instrumentação e interpretação

- **Vazão útil:** respostas corretas/s. O JSON informa conclusões dentro da janela de admissão e também a razão entre todos os sucessos e janela + drenagem. Os resultados do relatório usam a segunda medida para contar todo o tempo gasto com os pedidos admitidos.
- **Latência de sucesso:** da chamada ao SDK até receber e verificar a resposta. Rejeições têm um histograma separado. Não subtrair 10 ms de um percentil para chamar a diferença de latência do broker.
- **Carga aberta:** chegadas programadas independentemente das respostas, com limite de pendências no gerador. Registrar chegadas planejadas, envios, descartes locais, atraso de envio e latência desde o instante planejado. Descartes locais não são rejeições do VeloBus e não têm amostra de latência RPC.
- **Carga fechada:** cada vaga só manda o próximo pedido quando recebe uma resposta. Esse modelo desacelera quando o sistema fica lento e não representa sozinho uma taxa externa fixa. O problema de omitir esperas dessa forma é discutido pelo projeto [wrk2](https://github.com/giltene/wrk2); este teste usa seu próprio gerador para o protocolo VeloBus, não executa wrk2.
- **Histogramas:** memória limitada, resolução declarada no JSON. p99 não é máximo e percentis de execuções diferentes não devem ser somados ou tratados como uma população única.
- **CPU/RSS:** `ps` a cada 200 ms; CPU por diferença do contador acumulado, 100% equivale a um núcleo lógico. Linux costuma exibir CPU em segundos inteiros e macOS em centésimos; intervalos curtos sofrem quantização. RSS é máximo amostrado, não um limite garantido. A soma de RSS de processos pode contar páginas compartilhadas mais de uma vez.
- **Isolamento:** broker Rust, cada worker e gerador Node em processos separados. Todos compartilham a mesma máquina e loopback. O coordenador monitora recursos e stats; seu custo e a coleta fazem parte das condições do ensaio. O gerador também mede CPU, memória e atraso de seu event loop.
- **Verificação:** contadores do broker precisam bater com respostas e rejeições do cliente; execução dos handlers deve bater com respostas verificadas; concorrência não pode ultrapassar a declarada; fila, execução e bytes devem zerar após drenagem. Uma chamada extra verifica atendimento depois da carga, fora da medição.

Não há instrumentação separada de tempo em fila e execução no protocolo atual. Os picos de fila/RSS são amostrados; os workers contam cada entrada/saída para verificar seu próprio pico de handlers. O modo é memória, sem TLS e sem replicação. Estes ensaios não avaliam persistência, falha de rede, banco real, alta disponibilidade ou consumo de energia.

## Resultados

Os resultados medidos são adicionados após a execução validada, com ambiente, identificação do binário e dados por repetição. Até essa publicação, este arquivo descreve apenas o procedimento.

Raspberry Pi 5 continua sendo um alvo para executar a mesma suíte em Linux de 64 bits. Registrar temperatura, refrigeração, alimentação e throttling no hardware real. Testes funcionais em ARM64 no CI não equivalem a benchmark no Pi, e resultados de outro hardware não devem ser extrapolados.
