//! Default-feature compile contract; this does not count as FTS behavior.
use pi_agent::{
    EntrySearchHit, SearchQuery, SessionSearchHit, SessionSearchService, SessionSearchTopEntry,
};
fn schema_trait<S: SessionSearchService>(
    service: &S,
) -> &dyn pi_agent_schema::SessionSearchService<Error = S::Error> {
    service
}
#[test]
fn forwarded_search_contract_has_the_same_schema_identity() {
    let query = SearchQuery {
        text: "literal".into(),
        limit: None,
    };
    let _: pi_agent_schema::SearchQuery = query;
    let _: Option<pi_agent_schema::EntrySearchHit> = None::<EntrySearchHit>;
    let _: Option<pi_agent_schema::SessionSearchHit> = None::<SessionSearchHit>;
    let _: Option<pi_agent_schema::SessionSearchTopEntry> = None::<SessionSearchTopEntry>;
    // Taking the generic function's identity needs no substitute search provider.
    fn accepts<S: SessionSearchService>(s: &S) {
        let _ = schema_trait(s);
    }
    let _ = std::any::type_name::<SearchQuery>();
    #[allow(dead_code)]
    fn compile<S: SessionSearchService>(s: &S) {
        accepts(s);
    }
}
