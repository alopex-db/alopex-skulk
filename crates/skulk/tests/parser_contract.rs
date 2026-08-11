#[cfg(any(feature = "promql", feature = "sql-ts"))]
#[test]
fn active_descriptor_contract_reaches_runtime_parser() {
    #[cfg(feature = "promql")]
    let actual = alopex_skulk::query::promql::parser_contract_version().unwrap();
    #[cfg(all(not(feature = "promql"), feature = "sql-ts"))]
    let actual = alopex_skulk::query::sqlts::parser_contract_version().unwrap();

    assert_eq!(actual, env!("SKULK_NIM_PARSER_CONTRACT_VERSION"));
}
