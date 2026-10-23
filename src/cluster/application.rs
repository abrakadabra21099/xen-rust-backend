//! Ordered delivery of committed Paxos values to a node-local state machine.

use super::paxos_storage::{PaxosStorageError, SqlitePaxosStore};

/// A deterministic projection of the committed command log.
///
/// Implementations should treat `(slot, value)` as an idempotency key. A crash
/// after applying an effect but before `applied_index` advances causes that
/// slot to be delivered again after restart.
pub trait ReplicatedStateMachine {
    /// Applies one committed command. The byte format belongs to the concrete
    /// application state machine, not the Paxos transport.
    fn apply(&mut self, slot: u64, value: &[u8]) -> Result<(), String>;
}

/// Errors while delivering committed values to the application.
#[derive(Debug)]
pub enum ApplicationError {
    Storage(PaxosStorageError),
    Apply { slot: u64, message: String },
}

impl std::fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "application log storage failed: {error}"),
            Self::Apply { slot, message } => {
                write!(f, "application rejected committed slot {slot}: {message}")
            }
        }
    }
}

impl std::error::Error for ApplicationError {}

/// Delivers the next committed batch in order and persists successful progress.
pub struct CommittedLogApplier;

impl CommittedLogApplier {
    /// Applies at most `limit` values after the persisted application cursor.
    /// The cursor advances after each successful callback. If a callback fails,
    /// earlier slots in this batch remain applied and the failed slot is
    /// retried on the next call.
    pub fn apply_available(
        store: &mut SqlitePaxosStore,
        state_machine: &mut impl ReplicatedStateMachine,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        if limit == 0 {
            return Err(ApplicationError::Storage(
                PaxosStorageError::InvalidReadLimit,
            ));
        }
        let applied_index = store.applied_index().map_err(ApplicationError::Storage)?;
        let committed = store
            .committed_values_after(applied_index, limit)
            .map_err(ApplicationError::Storage)?;

        let mut applied_count = 0;
        for chosen in committed {
            state_machine
                .apply(chosen.slot, &chosen.value)
                .map_err(|message| ApplicationError::Apply {
                    slot: chosen.slot,
                    message,
                })?;
            store
                .mark_applied(chosen.slot)
                .map_err(ApplicationError::Storage)?;
            applied_count += 1;
        }
        Ok(applied_count)
    }
}
