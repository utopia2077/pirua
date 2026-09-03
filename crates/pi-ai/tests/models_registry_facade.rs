use pi_ai::{
    ModelsMemoryIntent, ModelsPersistenceIntent, ModelsPublication, ModelsRefreshOptions,
    ModelsRegistry, ModelsRegistryOptions, Provider, ProviderModelsRefresher, PublicationOutcome,
};

#[test]
fn registry_runtime_is_reexported_through_facade() {
    fn accepts_provider(_: &dyn Provider) {}
    fn accepts_refresher(_: &dyn ProviderModelsRefresher) {}
    fn accepts_registry(_: &ModelsRegistry) {}
    let _provider: fn(&dyn Provider) = accepts_provider;
    let _refresher: fn(&dyn ProviderModelsRefresher) = accepts_refresher;
    let _registry: fn(&ModelsRegistry) = accepts_registry;
    let _options: Option<ModelsRegistryOptions> = None;
    let _refresh = ModelsRefreshOptions::default();
    let _publication = ModelsPublication {
        memory: ModelsMemoryIntent::Keep,
        persistence: ModelsPersistenceIntent::Keep,
    };
    let _outcome = PublicationOutcome::Published;
}
