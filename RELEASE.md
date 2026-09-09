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

Estado: candidato validado; publicação e verificação a partir do registro npm pendentes.
