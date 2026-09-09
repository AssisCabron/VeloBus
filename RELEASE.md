# Nodara 0.3.0

Primeira versão publicada no npm como `nodara`, sob licença MIT. Nome anterior: VeloBus. O repositório mantém https://github.com/AssisCabron/VeloBus .

## Conteúdo

- Pool de até oito brokers independentes, seleção por pendências locais e alternância de empates.
- Reconexão limitada com jitter e restauração de registros dos workers.
- Concorrência de handlers compartilhada no processo, preservada durante quedas/reconexões.
- Reencaminhamento apenas após rejeição explícita antes do despacho; resultado incerto sem replay automático.
- Guia `LLM.txt`, índice `llms.txt`, exemplos e tipos TypeScript no pacote.
- Renomeação do pacote/binário para `nodara` e variáveis para `NODARA_*`.

Não inclui replicação de RPC/WAL, consenso, recuperação de chamadas perdidas ou garantia de execução exatamente uma vez. Os benchmarks históricos v0.2 não medem o pool.

## Validação antes de publicar

117 testes do produto e 15 do instrumento passaram. O tarball local foi instalado em projeto separado e validado com ESM, CommonJS, TypeScript, um broker, dois brokers e conferência de documentação/licença. Demonstração com três brokers e queda de um passou. rustfmt, Clippy e build release passaram.

## Publicação

Estado: **publicado** como [nodara@0.3.0](https://www.npmjs.com/package/nodara/v/0.3.0), em 9 de setembro de 2026, pela conta `assiscabron`, com licença MIT. A autenticação 2FA da publicação foi concluída.

Fonte do pacote: commit `ede58cd3dc538276d288baceb622901851b9dc63`. O [CI dessa fonte](https://github.com/AssisCabron/VeloBus/actions/runs/34401218767) passou em Linux x64 e ARM64.

Tarball validado: `nodara-0.3.0.tgz`, 23.320 bytes, SHA-1 `b46db19152793c165a80b67c220bc379bc0be9d0` e integridade npm `sha512-EGozhOC8ga0RvCWRZuqZRz0yIS2LW+LZ4waGS3QnVPfJp1xYAWXgLl3bX2iFdBCbDMW8KJTKMo6OH17G2QuNxg==`.

A integridade retornada pelo registro corresponde exatamente ao tarball validado. `NODARA_PACKAGE_SPEC=nodara@0.3.0 npm run test:package` passou: instalação limpa do registro, ESM, CommonJS, TypeScript, RPC com um e dois brokers e documentação/licença incluídas.

Instalação: `npm install nodara`. O broker Rust é executado separadamente.

O CI posterior da revisão `2269444` identificou uma disputa entre o prazo do broker (erro 10) e o cancelamento do worker (erro 12 com motivo de deadline) no teste de espera por capacidade. A asserção foi corrigida para aceitar esses dois resultados específicos, mantendo a verificação de que o handler expirado nunca executa. Essa correção altera somente o teste, sem modificar o pacote publicado.
