# Nodara 0.3.0

Primeira versão preparada para publicação no npm como `nodara`, sob licença MIT. Nome anterior: VeloBus. O repositório mantém https://github.com/AssisCabron/VeloBus .

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

Estado: candidato validado; o registro recusou a publicação com HTTP 403 por exigir autenticação adicional do mantenedor. Não houve publicação confirmada nem teste de instalação a partir do registro.

Fonte do pacote: commit `ede58cd3dc538276d288baceb622901851b9dc63`. O [CI dessa fonte](https://github.com/AssisCabron/VeloBus/actions/runs/34401218767) passou em Linux x64 e ARM64.

Tarball validado: `nodara-0.3.0.tgz`, 23.320 bytes, SHA-1 `b46db19152793c165a80b67c220bc379bc0be9d0` e integridade npm `sha512-EGozhOC8ga0RvCWRZuqZRz0yIS2LW+LZ4waGS3QnVPfJp1xYAWXgLl3bX2iFdBCbDMW8KJTKMo6OH17G2QuNxg==`.

Com a autenticação exigida pelo npm concluída, publicar o tarball testado com `npm publish ./artifacts/nodara-0.3.0.tgz --access public --registry=https://registry.npmjs.org/`. Depois, verificar a instalação remota com `NODARA_PACKAGE_SPEC=nodara@0.3.0 npm run test:package` e conferir a integridade no registro.
