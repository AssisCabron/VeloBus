# Validação da v0.2

Executada localmente em 9 de setembro de 2026, macOS ARM64 / Apple M2.

| Verificação | Resultado |
|---|---|
| Rust: RPC, transporte, protocolo de eventos, limites e WAL | 25 testes passaram |
| SDK: RPC, handlers, cancelamento, framing e eventos | 48 testes passaram |
| Integração por TCP e API HTTP real | 29 testes passaram |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passou |
| `cargo fmt --all --check` | Passou |
| Binário `release` local | Compilado |
| Instalação do pacote npm em projeto separado, ESM e CommonJS | Passou |
| Request/reply com o pacote instalado | Passou |
| Instrumento de benchmark: histogramas, chegadas, limites e leitura de recursos | 15 testes passaram |
| Benchmark curto de fumaça e execução curta dos sete cenários | Passaram |

Total: **102 testes do produto + 15 do instrumento de benchmark**, além dos testes de instalação e ensaios de carga. As execuções curtas validam o instrumento, não são resultados de desempenho.

Um teste de regressão reproduz um cliente que recebe a resposta e imediatamente repõe uma das 32 operações da conexão. A vaga e o identificador agora são liberados antes de tornar a resposta visível; a ordem anterior podia rejeitar esse cliente indevidamente e desconectar um worker do SDK sob carga. A saída permanece limitada. O teste falhou com a ordem antiga e passou com a correção.

A integração cobre proteção de concorrência e fila, expiração antes do despacho, preservação de capacidade durante handlers atrasados, desconexões, réplicas sem broadcast, 32 slots simultâneos, erros de serviço e HTTP 200/503/504. O núcleo de eventos também continua validado, inclusive recuperação de lote confirmado após SIGKILL, falhas de escrita, limites de capacidade e corrupção do WAL. SIGKILL não simula corte de energia.

O ensaio de 64 chamadas simultâneas respeitou quatro execuções máximas, completou 12 chamadas e rejeitou 52 com OVERLOADED. Ao final não havia chamadas ou bytes RPC pendentes. Veja [RPC-VALIDATION.md](RPC-VALIDATION.md).

```sh
npm run setup
npm test
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
npm run build
npm run test:package
npm run load:rpc
npm run test:benchmark
```

O workflow GitHub Actions inclui Linux x64 e ARM64. O resultado de execução remota deve ser verificado no GitHub; os resultados acima são locais. Não houve publicação no npm, implantação externa, teste no Raspberry Pi ou avaliação de alta disponibilidade. As medições históricas de eventos v0.1 não representam desempenho RPC.
