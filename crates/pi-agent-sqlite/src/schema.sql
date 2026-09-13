CREATE TABLE entries(id TEXT PRIMARY KEY, parent_id TEXT, seq INTEGER NOT NULL, type TEXT NOT NULL, custom_type TEXT, timestamp INTEGER NOT NULL, payload TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX ix_entry_parent ON entries(parent_id);
CREATE INDEX ix_entry_seq ON entries(seq,type);
CREATE TABLE registers(namespace TEXT NOT NULL,key TEXT NOT NULL,seq INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(namespace,key));
CREATE TABLE usage_ledger(id TEXT PRIMARY KEY,seq INTEGER NOT NULL,entry_id TEXT,adjustment INTEGER NOT NULL,usage TEXT NOT NULL,details TEXT) WITHOUT ROWID;
CREATE INDEX ix_usage_seq ON usage_ledger(seq);
CREATE TABLE branch_entries(branch_id TEXT NOT NULL,entry_id TEXT NOT NULL,entry_seq INTEGER NOT NULL,entry_type TEXT NOT NULL,row_digest BLOB NOT NULL,PRIMARY KEY(branch_id,entry_id)) WITHOUT ROWID;
CREATE INDEX ix_be_seq ON branch_entries(branch_id,entry_seq,entry_id,entry_type);
CREATE INDEX ix_be_type ON branch_entries(branch_id,entry_type,entry_seq,entry_id);
CREATE INDEX ix_be_entry ON branch_entries(entry_id);
CREATE TABLE branch_meta(branch_id TEXT PRIMARY KEY,tip_entry_id TEXT NOT NULL,tip_seq INTEGER NOT NULL,base_branch_id TEXT,base_seq INTEGER,row_count INTEGER NOT NULL,row_digest BLOB NOT NULL,checked_revision INTEGER NOT NULL,cache_revision INTEGER NOT NULL CHECK(cache_revision>=0 AND cache_revision<=9007199254740991),meta_digest BLOB NOT NULL);
CREATE UNIQUE INDEX ix_bm_tip ON branch_meta(tip_entry_id);
CREATE TABLE session(singleton INTEGER PRIMARY KEY CHECK(singleton=1),id TEXT NOT NULL,created_at INTEGER NOT NULL,parent_session_id TEXT,storage_version INTEGER NOT NULL,metadata TEXT NOT NULL,message_count INTEGER NOT NULL,usage_payload TEXT NOT NULL,next_seq INTEGER,store_generation INTEGER NOT NULL,format_version INTEGER NOT NULL,store_token BLOB NOT NULL,revision INTEGER NOT NULL,fence_counter INTEGER NOT NULL);
CREATE TABLE writer_lease(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id BLOB NOT NULL,fence INTEGER NOT NULL,expires_at_ms INTEGER NOT NULL);

CREATE TRIGGER branch_insert_check AFTER INSERT ON branch_entries BEGIN UPDATE branch_meta SET cache_revision=cache_revision+1 WHERE branch_id=NEW.branch_id; END;
CREATE TRIGGER branch_delete_check AFTER DELETE ON branch_entries BEGIN UPDATE branch_meta SET cache_revision=cache_revision+1 WHERE branch_id=OLD.branch_id; END;
CREATE TRIGGER branch_update_check AFTER UPDATE ON branch_entries BEGIN UPDATE branch_meta SET cache_revision=cache_revision+1 WHERE branch_id=OLD.branch_id OR branch_id=NEW.branch_id; END;
