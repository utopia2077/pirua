//! Application-owned persistent adapter for the existing credential port.
use pi_ai::*;
use pi_sqlite::{Database, OptionalExtension, params};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// One application database, serialized mutation leases and definitive SQLite commits.
#[derive(Clone)]
pub struct Credentials {
    database: Database,
    gate: Arc<Mutex<()>>,
}
impl Credentials {
    pub fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if !path.exists() {
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let database = Database::open(path)?;
        database.initialize("acp-credentials",1,"CREATE TABLE acp_credentials (provider TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL CHECK(revision>0), value TEXT) STRICT;")?;
        Ok(Self {
            database,
            gate: Arc::new(Mutex::new(())),
        })
    }
    fn load(
        &self,
        provider: &ProviderId,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        let row = self
            .database
            .read::<_, pi_sqlite::Error>(|c| {
                Ok(c.query_row(
                    "SELECT revision,value FROM acp_credentials WHERE provider=?1",
                    [provider.as_str()],
                    |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, Option<String>>(1)?)),
                )
                .optional()?)
            })
            .map_err(CredentialStoreError::with_source)?;
        row.and_then(|(r, v)| v.map(|v| (r, v)))
            .map(|(r, v)| {
                Ok(VersionedCredential {
                    revision: CredentialRevision::new(r).ok_or_else(|| {
                        CredentialStoreError::new(CredentialStoreErrorCode::Failed)
                    })?,
                    credential: serde_json::from_str(&v)
                        .map_err(CredentialStoreError::with_source)?,
                })
            })
            .transpose()
    }
    fn write(
        &self,
        provider: &ProviderId,
        next: CredentialMutationValue,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        if next == CredentialMutationValue::Unchanged {
            return self.load(provider);
        }
        let credential = match next {
            CredentialMutationValue::Set(c) => Some(c),
            _ => None,
        };
        let value = credential
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(CredentialStoreError::with_source)?;
        let revision=self.database.transaction::<_,pi_sqlite::Error>(|tx|{
            let old:Option<i64>=tx.query_row("SELECT revision FROM acp_credentials WHERE provider=?1",[provider.as_str()],|r|r.get(0)).optional()?;
            let revision=old.unwrap_or(0).checked_add(1).ok_or(pi_sqlite::Error::Poisoned)?;
            tx.execute("INSERT INTO acp_credentials(provider,revision,value) VALUES (?1,?2,?3) ON CONFLICT(provider) DO UPDATE SET revision=excluded.revision,value=excluded.value",params![provider.as_str(),revision,value])?;
            Ok(revision)
        }).map_err(CredentialStoreError::with_source)?;
        Ok(credential.map(|credential| VersionedCredential {
            revision: CredentialRevision::new(revision as u64).expect("positive revision"),
            credential,
        }))
    }
    /// Explicit one-time import; never probes an upstream Pi credential location.
    pub async fn import(
        &self,
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let entries: BTreeMap<String, Credential> = serde_json::from_slice(&std::fs::read(path)?)?;
        for (id, credential) in entries {
            let provider = ProviderId::new(id)?;
            let c = CancellationToken::new();
            let lease = self.begin_mutation(&provider, &c).await?;
            if lease.current().is_none() {
                lease
                    .commit(CredentialMutationValue::Set(credential))
                    .await?;
            }
        }
        Ok(())
    }
}
struct Lease {
    store: Credentials,
    provider: ProviderId,
    current: Option<VersionedCredential>,
    cancellation: CancellationToken,
    _guard: OwnedMutexGuard<()>,
}
impl CredentialMutation for Lease {
    fn current(&self) -> Option<&VersionedCredential> {
        self.current.as_ref()
    }
    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            check(&self.cancellation)?;
            self.store.write(&self.provider, next)
        })
    }
}
fn check(c: &CancellationToken) -> Result<(), CredentialStoreError> {
    if c.is_cancelled() {
        Err(CredentialStoreError::new(
            CredentialStoreErrorCode::Cancelled,
        ))
    } else {
        Ok(())
    }
}
impl CredentialStore for Credentials {
    fn read<'a>(
        &'a self,
        p: &'a ProviderId,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            check(c)?;
            self.load(p)
        })
    }
    fn list<'a>(
        &'a self,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async move {
            check(c)?;
            let ids=self.database.read::<_,pi_sqlite::Error>(|db|{let mut s=db.prepare("SELECT provider FROM acp_credentials WHERE value IS NOT NULL ORDER BY rowid")?;Ok(s.query_map([],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?)}).map_err(CredentialStoreError::with_source)?;
            ids.into_iter()
                .map(|id| {
                    let p = ProviderId::new(id).map_err(CredentialStoreError::with_source)?;
                    let v = self.load(&p)?.ok_or_else(|| {
                        CredentialStoreError::new(CredentialStoreErrorCode::Failed)
                    })?;
                    Ok(CredentialInfo {
                        provider_id: p,
                        auth_type: match v.credential {
                            Credential::ApiKey(_) => AuthType::ApiKey,
                            Credential::OAuth(_) => AuthType::OAuth,
                        },
                    })
                })
                .collect()
        })
    }
    fn begin_mutation<'a>(
        &'a self,
        p: &'a ProviderId,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        Box::pin(async move {
            check(c)?;
            let guard = tokio::select! {g=self.gate.clone().lock_owned()=>g,_=c.cancelled()=>return Err(CredentialStoreError::new(CredentialStoreErrorCode::Cancelled))};
            check(c)?;
            Ok(Box::new(Lease {
                store: self.clone(),
                provider: p.clone(),
                current: self.load(p)?,
                cancellation: c.clone(),
                _guard: guard,
            }) as Box<dyn CredentialMutation>)
        })
    }
    fn reconcile<'a>(
        &'a self,
        _p: &'a ProviderId,
        _r: CredentialReconciliation,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            check(c)?;
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::ReconciliationNotRequired,
            ))
        })
    }
}
