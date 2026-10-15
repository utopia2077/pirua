use pi_sqlite::{Database, Error};

#[test]
fn component_schemas_share_one_connection_and_rollback_failed_initialization() {
    let database = Database::memory().unwrap();
    database
        .initialize("one", 1, "CREATE TABLE one(value TEXT) STRICT;")
        .unwrap();
    database
        .initialize("two", 2, "CREATE TABLE two(value TEXT) STRICT;")
        .unwrap();
    assert!(
        database
            .initialize(
                "broken",
                1,
                "CREATE TABLE partial(value TEXT) STRICT; THIS IS INVALID SQL;"
            )
            .is_err()
    );
    database
        .read::<_, Error>(|connection| {
            let count: i64 = connection.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='partial'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0);
            let count: i64 =
                connection.query_row("SELECT COUNT(*) FROM pirua_schema", [], |row| row.get(0))?;
            assert_eq!(count, 2);
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_multi_query_snapshot_does_not_mix_external_commits() {
    let file = std::env::temp_dir().join(format!(
        "pirua-sqlite-snapshot-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let reader = Database::open(&file).unwrap();
        let writer = Database::open(&file).unwrap();
        reader
            .initialize(
                "test",
                1,
                "CREATE TABLE values_for_test(value INTEGER) STRICT;",
            )
            .unwrap();
        reader
            .snapshot::<_, Error>(|transaction| {
                let before: i64 =
                    transaction
                        .query_row("SELECT COUNT(*) FROM values_for_test", [], |row| row.get(0))?;
                writer.transaction::<_, Error>(|transaction| {
                    transaction.execute("INSERT INTO values_for_test VALUES (1)", [])?;
                    Ok(())
                })?;
                let after: i64 =
                    transaction
                        .query_row("SELECT COUNT(*) FROM values_for_test", [], |row| row.get(0))?;
                assert_eq!(before, 0);
                assert_eq!(after, 0);
                Ok(())
            })
            .unwrap();
        reader
            .snapshot::<_, Error>(|transaction| {
                assert_eq!(
                    transaction.query_row("SELECT COUNT(*) FROM values_for_test", [], |row| row
                        .get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .unwrap();
    }
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", file.display()));
    }
}
