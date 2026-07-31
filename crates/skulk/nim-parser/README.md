# Vendored Nim Query Parser

Skulk's `promql` and `sql-ts` features link the Alopex Nim query parser from
`vendor/<target-triple>/`. The source of truth is
`alopex/crates/alopex-sql/nim-sql-parser`.

The vendored contract version must match the value returned by
`alopex_parser_version()`. Rebuild the library from the Alopex repository,
copy the platform artifact here, update `CONTRACT_VERSION` and `SHA256SUMS`,
then run Skulk's frontend feature tests.
