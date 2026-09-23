//! Executor-neutral process-local controls, independent of concrete driver ownership.

mod admission;
mod mutation_line;

pub use admission::{Admission, CommitPermit};
pub use mutation_line::{Acquire, MutationLine, MutationPermit, PairAcquire, PairPermit};

mod supervisor;
pub use supervisor::{Supervisor, TaskTicket};

mod completion;
pub use completion::retained;

pub(crate) use completion::catch_task;
