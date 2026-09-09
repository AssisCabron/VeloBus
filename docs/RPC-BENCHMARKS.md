> Registro histórico anterior à renomeação para Nodara. Binários, variáveis e resultados abaixo pertencem à versão indicada. Na árvore atual, use `nodara` e o prefixo `NODARA_`; não atribua estas medições ao pool v0.3.

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

Execução local em **9 de setembro de 2026**, Apple M2, 8 CPUs lógicas, 8 GiB RAM, macOS/Darwin 27.0.0 ARM64, Node 26.7.0 e Rust 1.97.1. Desktop compartilhado, sem isolamento de CPU. Foram 21 execuções: sete cenários × três repetições, cada uma com 2 s de aquecimento e 10 s de admissão, seguidos da drenagem.

Fonte medida: [`6b3a792`](https://github.com/AssisCabron/VeloBus/commit/6b3a79282152672092ab982eaabfa2c57f27f7a6), árvore de trabalho limpa no início. Binário release de **1.197.200 bytes**, SHA-256 `3a128c8c642ed1d7253378005a746c63c1a8d4a510b995f22f07f3df751a0152`. O [JSON completo](benchmarks/local-rpc.json) contém configurações, resultados por execução e metadados de precisão.

### Respostas completas e latência

Medianas de três execuções, com mínimo–máximo entre elas. A coluna p99 é a **mediana dos p99 de sucesso de cada execução**, não o p99 de uma população combinada. Latências em milissegundos; histogramas com aproximação de até 1% acima de 0,01 ms.

| Cenário | Respostas corretas/s, mediana | Faixa entre execuções | p99 de sucesso, mediana | Faixa de p99 |
| --- | ---: | ---: | ---: | ---: |
| echo-c1 | 10.354 | 9.966–12.158 | 0,322 ms | 0,179–0,339 ms |
| echo-c32 | **48.288** | 43.340–52.931 | **1,522 ms** | 1,186–2,334 ms |
| echo-c128 | 48.163 | 36.903–48.704 | 6,702 ms | 6,376–14,419 ms |
| echo-64k | 14.414 | 12.957–14.498 | 4,874 ms | 4,826–7,329 ms |
| slow-normal | 799 | 798,9–799,3 | 11,935 ms | 11,817–11,935 ms |
| slow-overload | 1.406 | 1.395–1.422 | 36,016 ms | 35,306–37,107 ms |
| slow-scale-out | 2.831 | 2.734–2.834 | 23,713 ms | 23,713–28,365 ms |

O cenário de 128 chamadas e quatro workers teve praticamente a mesma vazão mediana do cenário de 32 chamadas e um worker, com p99 maior. Esse resultado não justifica aumentar concorrência indiscriminadamente. A primeira repetição dos cenários leves foi mais lenta que outras repetições; as faixas são parte do resultado, sem descartar execuções desfavoráveis.

No serviço simulado de 10 ms, passar de dois para quatro workers, mantendo oito vagas por worker, elevou a vazão mediana em aproximadamente **2,01×**. Isso demonstra escala de workers nessa carga de espera por I/O, sem demonstrar escala linear para trabalho de CPU ou eliminação do broker único como ponto de falha.

**p99 não é garantia de latência máxima:** foram observadas respostas de até 300,738 ms. O maior máximo no cenário `echo-c32` foi 209,750 ms. Esta medição curta em desktop não certifica um SLO de produção.

### Sobrecarga e qualidade da geração de carga

Contagens somadas das três repetições; aquecimento excluído. Nenhuma chamada foi repetida automaticamente.

| Cenário aberto | Chegadas planejadas | Enviadas ao SDK | Respostas corretas | OVERLOADED do broker | Descartes do gerador |
| --- | ---: | ---: | ---: | ---: | ---: |
| slow-normal | 24.000 | 23.999 | 23.999 | 0 | 1 |
| slow-overload | 120.000 | 119.606 | 42.368 | 77.238 | 394 |
| slow-scale-out | 120.000 | 118.995 | 84.133 | 34.862 | 1.005 |

O limite de fila compartilhada foi atingido em 32 chamadas, com 16 execuções atribuídas no cenário de dois workers e 32 no de quatro workers. Cada worker lento observou no máximo oito handlers ativos. Após cada execução, chamadas em fila, em execução e bytes RPC retidos voltaram a zero; a chamada de verificação posterior também passou.

O gerador não sustentou a agenda inteira em algumas pausas. Houve **394 descartes de 120.000 chegadas** no cenário com dois workers e **1.005 de 120.000** no de quatro. Na pior repetição, foram 997/40.000 (2,49%): 835 por atraso acima da janela de recuperação, 158 por limite local de pendências e quatro no encerramento da janela. Esses pedidos **não foram enviados nem rejeitados pelo VeloBus**. Os percentis não contêm amostras para eles; não apresentar a taxa nominal de 4.000/s como taxa integralmente entregue ao broker.

Contando desde o instante planejado de chegada, as medianas dos p99 de sucesso foram 12,796 ms (`slow-normal`), 36,740 ms (`slow-overload`) e 24,432 ms (`slow-scale-out`). O maior p99 de rejeição em uma repetição foi 4,412 ms; rejeições continuam fora do histograma de sucesso. As causas das pausas do gerador não foram identificadas por perfilamento.

### CPU e memória

CPU do broker: mediana da média por execução, em percentual de **um núcleo lógico**. RSS: maior valor amostrado entre as três execuções de cada cenário. São medidas de processos locais e não previsão de consumo em outro sistema operacional.

| Cenário | CPU média do broker | RSS do broker | Maior RSS de um worker Node | RSS do gerador Node |
| --- | ---: | ---: | ---: | ---: |
| echo-c1 | 67,0% | 2,91 MiB | 69,17 MiB | 73,88 MiB |
| echo-c32 | 275,9% | 2,66 MiB | 84,63 MiB | 86,19 MiB |
| echo-c128 | 324,8% | 3,31 MiB | 74,05 MiB | 124,72 MiB |
| echo-64k | 244,6% | 10,91 MiB | 202,06 MiB | 231,84 MiB |
| slow-normal | 11,8% | 3,08 MiB | 66,06 MiB | 66,94 MiB |
| slow-overload | 23,3% | 3,14 MiB | 66,81 MiB | 73,91 MiB |
| slow-scale-out | 29,2% | 3,14 MiB | 66,11 MiB | 74,23 MiB |

O RSS pequeno do broker **não é o consumo de todo o sistema**: existem de um a quatro workers, o gerador e o coordenador. O JSON registra cada processo individualmente. Não somar os máximos acima como se fossem um pico simultâneo. No payload de 64 KiB, o gerador consumiu aproximadamente um núcleo e os processos Node tiveram RSS maior; o gargalo exato ainda requer perfilamento de CPU/alocações e ensaio com gerador em outra máquina.

### Validação e próximos experimentos

As 21 execuções passaram: **3.678.019 respostas verificadas**, 112.100 rejeições explícitas por sobrecarga e nenhum outro erro de RPC. Os contadores de execução do worker e do broker conferiram com os resultados do cliente. Não houve divergência de payload ou identificador.

O benchmark curto inicial revelou uma corrida na liberação da vaga de transporte. Ela foi corrigida em [`24bc1dd`](https://github.com/AssisCabron/VeloBus/commit/24bc1dd41d4613a63d80f32c8e93e16facd306fc), com teste determinístico que falhou antes da correção. Todos os números acima usam a versão corrigida. A [validação funcional](VALIDATION.md) inclui 102 testes do produto e 15 do instrumento, além da instalação do pacote e dos ensaios curtos. O [CI da fonte medida](https://github.com/AssisCabron/VeloBus/actions/runs/34397665852) passou em Linux x64 e ARM64.

Próximos experimentos recomendados: perfilamento do caminho `REQUEST`/`TAKE`/`COMPLETE`, payload JSON com serviço real, carga e workers em hosts separados, mistura de rotas e ensaio mais longo. As [features propostas](FEATURES.md) usam essa referência para avaliar custo de instrumentação, justiça entre rotas e uma possível operação combinada de conclusão/coleta.

Raspberry Pi 5 continua sendo um alvo para executar a mesma suíte em Linux de 64 bits. Registrar temperatura, refrigeração, alimentação e throttling no hardware real. Testes funcionais em ARM64 no CI não equivalem a benchmark no Pi, e resultados de outro hardware não devem ser extrapolados.
