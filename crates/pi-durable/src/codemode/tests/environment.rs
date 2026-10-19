use super::*;
use pi_ai::BoxFuture;
use pi_coding_runtime::tools::*;
pub(super) struct RecordingEnv {
    pub local: LocalExecutionEnv,
    pub events: Arc<Events>,
}
impl ExecutionEnv for RecordingEnv {
    fn absolute_path<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.local.absolute_path(p)
    }
    fn exists<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<bool>> {
        self.local.exists(p)
    }
    fn read_binary_file<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>> {
        self.local.read_binary_file(p)
    }
    fn read_text_file<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.local.read_text_file(p)
    }
    fn write_file<'a>(&'a self, p: &'a str, b: &'a [u8]) -> BoxFuture<'a, EnvResult<()>> {
        Box::pin(async move {
            let storage = self.events.storage.lock().unwrap().clone().unwrap();
            let id = self.events.code_task.load(Ordering::SeqCst);
            let run = snapshot(
                storage.as_ref(),
                codemode_run_doc(),
                Scope::Task {
                    task_id: TaskId::new(id).unwrap(),
                },
            );
            self.events
                .files
                .lock()
                .unwrap()
                .push(json!({"kind":"writeFile","path":p,"bytes":b,"committedRun":run}));
            self.local.write_file(p, b).await
        })
    }
    fn list_dir<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<Vec<FileInfo>>> {
        self.local.list_dir(p)
    }
    fn file_info<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<FileInfo>> {
        self.local.file_info(p)
    }
    fn canonical_path<'a>(&'a self, p: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.local.canonical_path(p)
    }
    fn exec<'a>(
        &'a self,
        s: &'a str,
        c: &'a str,
        o: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>> {
        self.local.exec(s, c, o)
    }
    fn create_temp_file<'a>(&'a self, p: &'a str, b: &'a [u8]) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move {
            let result = self.local.create_temp_file(p, b).await?;
            self.events
                .files
                .lock()
                .unwrap()
                .push(json!({"kind":"reserve","path":result,"bytes":b}));
            Ok(result)
        })
    }
}
