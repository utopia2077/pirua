//! Independently written finite-state oracle; no production transition/query helpers.
use crate::support::backend::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use std::collections::BTreeMap;

#[derive(Default)]
struct Model {
    next: u64,
    registers: BTreeMap<&'static str, (u64, bool)>,
    occupied: bool,
    entry: bool,
    ledger: bool,
}
#[derive(Debug, PartialEq)]
enum ResultKind {
    Applied,
    NotApplied,
    Corruption,
}
impl Model {
    fn step(&mut self, action: u8) -> ResultKind {
        match action {
            0 => {
                self.registers.insert("k", (self.next, false));
                self.next += 1;
            }
            1 => {
                self.registers.remove("k");
                self.next += 1;
            }
            2 => {
                self.registers.insert("other", (self.next, true));
                self.next += 1;
            }
            3 | 4 => {
                let matches = if action == 3 {
                    !self.registers.contains_key("k")
                } else {
                    self.registers.get("k").is_some_and(|(seq, _)| *seq == 0)
                };
                if !matches {
                    return ResultKind::NotApplied;
                }
                self.registers.insert("k", (self.next, true));
                self.next += 1;
            }
            5 | 6 => {
                // The candidate first writes a register, then inserts into the shared ID domain.
                // A failed second write must not publish that prefix or its allocation.
                if self.occupied {
                    return ResultKind::Corruption;
                }
                self.registers.insert("prefix", (self.next, false));
                self.next += 2;
                self.occupied = true;
                self.entry = action == 5;
                self.ledger = action == 6;
            }
            7 => {} // empty transaction
            _ => unreachable!(),
        }
        ResultKind::Applied
    }
}
pub fn enumerate(factory: &impl BackendFactory) -> usize {
    let mut steps = 0;
    for encoded in 0u32..8u32.pow(4) {
        let store = factory.create();
        let mut model = Model::default();
        let mut trace = encoded;
        for _ in 0..4 {
            let action = (trace % 8) as u8;
            trace /= 8;
            let expected = model.step(action);
            let mut conditions = vec![];
            let writes = match action {
                0 => vec![set("k", JsonValue::Bool(false))],
                1 => vec![delete("k")],
                2 => vec![set("other", JsonValue::Bool(true))],
                3 | 4 => {
                    conditions.push(condition(
                        "k",
                        if action == 3 {
                            ExpectedRegister::Absent
                        } else {
                            ExpectedRegister::Seq(n(0))
                        },
                    ));
                    vec![set("k", JsonValue::Bool(true))]
                }
                5 => vec![
                    set("prefix", JsonValue::Bool(false)),
                    custom("shared", None),
                ],
                6 => vec![
                    set("prefix", JsonValue::Bool(false)),
                    usage("shared", Usage::zero(), None),
                ],
                7 => vec![],
                _ => unreachable!(),
            };
            let before = snapshot_bytes(factory, &*store);
            let actual = factory.execute(store.commit_conditional(ConditionalCommit {
                conditions,
                transaction: tx(writes),
            }));
            let kind = match actual {
                Ok(ConditionalOutcome::Applied(_)) => ResultKind::Applied,
                Ok(ConditionalOutcome::NotApplied) => ResultKind::NotApplied,
                Err(StorageError::Corruption(_)) => ResultKind::Corruption,
                other => panic!("unexpected model result {other:?}"),
            };
            assert_eq!(kind, expected, "trace {encoded} action {action}");
            if kind != ResultKind::Applied {
                assert_eq!(
                    snapshot_bytes(factory, &*store),
                    before,
                    "failed trace consumed authority"
                );
            }
            let snapshot = factory.execute(store.snapshot()).unwrap();
            assert_eq!(snapshot.next_seq, Some(n(model.next)));
            let observed: BTreeMap<_, _> = snapshot
                .registers
                .iter()
                .map(|register| match register {
                    Register::FactCustom {
                        key,
                        value: JsonValue::Bool(value),
                        seq,
                    } => (key.as_str(), (seq.get(), *value)),
                    _ => panic!("unexpected model register"),
                })
                .collect();
            assert_eq!(observed, model.registers);
            assert_eq!(snapshot.entries.len(), usize::from(model.entry));
            assert_eq!(snapshot.usage.len(), usize::from(model.ledger));
            assert_eq!(snapshot.stats.usage, Usage::zero());
            steps += 1;
        }
    }
    steps
}
