//! Bounded coherent hydration of configured lane roots and their named values.
use crate::scheduler::Admission;
use pi_agent_schema::*;
use pi_agent_session::{session::RuntimeSession, storage::RuntimeStorage};
use pi_harness_core::current_validation::{
    CurrentRoots, collect_current_references, validate_current_entries_and_registers,
};
use std::collections::BTreeMap;

/// A coherent read retains every root token alongside named hydration.
#[derive(Clone, Debug)]
pub struct HydratedLane {
    /// Exact configuration, lane, leaf and operation register values.
    pub roots: CurrentRoots,
    /// Bounded named committed entries, including materialized reservations.
    pub entries: BTreeMap<EntryId, Entry>,
    /// Exact named mutable register values and sequence tokens.
    pub registers: BTreeMap<(RegisterNamespace, String), Register>,
    /// Pure validated operation projection; absent denotes a configured idle lane.
    pub current: Option<CurrentOperation>,
}

pub(crate) fn fault(message: &'static str) -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new(message).expect("bounded static runtime error"),
    ))
}

async fn attempt<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    session: &RuntimeSession<S, M>,
    lane: LaneId,
) -> Result<HydratedLane, HarnessRuntimeError> {
    let required = |value: Option<Register>| {
        value.ok_or_else(|| fault("configured runtime lane root is missing"))
    };
    let configuration = required(
        session
            .get_register(RegisterNamespace::LaneConfig, lane.as_str())
            .await
            .map_err(|_| fault("runtime root read failed"))?,
    )?;
    let lane_state = required(
        session
            .get_register(RegisterNamespace::LaneState, lane.as_str())
            .await
            .map_err(|_| fault("runtime root read failed"))?,
    )?;
    let leaf = required(
        session
            .get_register(RegisterNamespace::LaneLeaf, lane.as_str())
            .await
            .map_err(|_| fault("runtime root read failed"))?,
    )?;
    let Register::LaneState { value, .. } = &lane_state else {
        return Err(fault("runtime lane state has incorrect kind"));
    };
    let (operation_meta, operation_state) = if let Some(id) = &value.current_operation_id {
        (
            session
                .get_register(RegisterNamespace::OperationMeta, id.as_str())
                .await
                .map_err(|_| fault("runtime operation read failed"))?,
            session
                .get_register(RegisterNamespace::OperationState, id.as_str())
                .await
                .map_err(|_| fault("runtime operation read failed"))?,
        )
    } else {
        (None, None)
    };
    let roots = CurrentRoots {
        lane,
        configuration,
        lane_state,
        leaf,
        operation_meta,
        operation_state,
    };
    let references = collect_current_references(&roots)
        .map_err(|_| fault("runtime current roots are invalid"))?;
    let ids: Vec<_> = references.entries.keys().cloned().collect();
    let entries = session
        .get_entries(&ids)
        .await
        .map_err(|_| fault("runtime entry hydration failed"))?;
    let mut registers = BTreeMap::new();
    for (namespace, key) in references.registers.keys() {
        if let Some(register) = session
            .get_register(*namespace, key)
            .await
            .map_err(|_| fault("runtime register hydration failed"))?
        {
            registers.insert((*namespace, key.clone()), register);
        }
    }
    let current = validate_current_entries_and_registers(&roots, &entries, &registers)
        .map_err(|_| fault("runtime current graph is invalid"))?;
    Ok(HydratedLane {
        roots,
        entries,
        registers,
        current,
    })
}

/// Reads only roots and their bounded reference inventory. Provisional errors are
/// discarded only after an actual stamp mismatch; stable errors are returned.
pub async fn hydrate_lane<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    session: &RuntimeSession<S, M>,
    lane: LaneId,
    admission: &Admission,
) -> Result<HydratedLane, HarnessRuntimeError> {
    loop {
        admission.check()?;
        let before = session
            .read_stamp()
            .await
            .map_err(|_| fault("runtime read stamp failed"))?;
        let candidate = attempt(session, lane.clone()).await;
        let after = session
            .read_stamp()
            .await
            .map_err(|_| fault("runtime read stamp failed"))?;
        if before == after {
            return candidate;
        }
    }
}

/// Discovers and hydrates all configured lanes under a single coherent stamp.
/// It performs no history, usage, last-result, context or external-effect reads.
pub async fn restore_configured<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    session: &RuntimeSession<S, M>,
    admission: &Admission,
) -> Result<BTreeMap<LaneId, HydratedLane>, HarnessRuntimeError> {
    loop {
        admission.check()?;
        let before = session
            .read_stamp()
            .await
            .map_err(|_| fault("runtime read stamp failed"))?;
        let candidate = async {
            let roots = session
                .list_registers(RegisterNamespace::LaneState, None)
                .await
                .map_err(|_| fault("runtime lane discovery failed"))?;
            let mut lanes = BTreeMap::new();
            for root in roots {
                let Register::LaneState { key, .. } = root else {
                    return Err(fault("runtime lane discovery has incorrect kind"));
                };
                let lane =
                    LaneId::new(key).map_err(|_| fault("runtime lane identity is invalid"))?;
                let value = attempt(session, lane.clone()).await?;
                lanes.insert(lane, value);
            }
            if !lanes.contains_key(&LaneId::new("main").expect("static lane")) {
                return Err(fault("configured main lane is required"));
            }
            Ok(lanes)
        }
        .await;
        let after = session
            .read_stamp()
            .await
            .map_err(|_| fault("runtime read stamp failed"))?;
        if before == after {
            return candidate;
        }
    }
}

/// Seed only the explicitly authorized, idle, unconfigured main lane. Seed
/// publication intentionally precedes full restore; later restore errors do not
/// roll it back. This reads no history, usage, model or application context.
pub(crate) async fn initialize_main<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    session: &RuntimeSession<S, M>,
    seed: &LaneConfiguration,
    telemetry: &dyn pi_telemetry::TelemetryContext,
    admission: &Admission,
) -> Result<(), HarnessRuntimeError> {
    use pi_agent_session::storage::{
        ConditionalCommit, ConditionalOutcome, ExpectedRegister, RegisterCondition, RegisterKey,
    };
    loop {
        admission.check()?;
        let before = session
            .read_stamp()
            .await
            .map_err(|_| fault("initialization stamp read failed"))?;
        let candidate = async {
            if session
                .get_register(RegisterNamespace::LaneConfig, "main")
                .await
                .map_err(|_| fault("initialization configuration read failed"))?
                .is_some()
            {
                return Ok(None);
            }
            let state = session
                .get_register(RegisterNamespace::LaneState, "main")
                .await
                .map_err(|_| fault("initialization state read failed"))?
                .ok_or_else(|| fault("initialization main state missing"))?;
            let leaf = session
                .get_register(RegisterNamespace::LaneLeaf, "main")
                .await
                .map_err(|_| fault("initialization leaf read failed"))?
                .ok_or_else(|| fault("initialization main leaf missing"))?;
            let idle_main = matches!(&state, Register::LaneState { key, value, .. }
                if key == "main" && value.current_operation_id.is_none());
            let main_leaf = matches!(&leaf, Register::LaneLeaf { key, .. } if key == "main");
            if !idle_main || !main_leaf {
                return Err(fault("initialization requires actual idle main roots"));
            }
            Ok(Some(ConditionalCommit {
                conditions: vec![
                    RegisterCondition {
                        register: RegisterKey {
                            namespace: RegisterNamespace::LaneConfig,
                            key: "main".into(),
                        },
                        expected: ExpectedRegister::Absent,
                    },
                    crate::publication::observed(&state),
                    crate::publication::observed(&leaf),
                ],
                transaction: Transaction {
                    writes: vec![Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                        key: "main".into(),
                        value: seed.clone(),
                    }))],
                },
            }))
        }
        .await;
        let after = session
            .read_stamp()
            .await
            .map_err(|_| fault("initialization stamp read failed"))?;
        if before != after {
            continue;
        }
        let Some(request) = candidate? else {
            return Ok(());
        };
        let _permit = admission.try_admit()?;
        match session
            .commit_conditional_with_telemetry(request, telemetry)
            .await
            .map_err(|_| fault("initialization conditional publication failed"))?
        {
            ConditionalOutcome::Applied(_) => return Ok(()),
            ConditionalOutcome::NotApplied => {}
        }
    }
}
