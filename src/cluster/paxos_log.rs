//! Client-operation submission across the ordered, leaderless Paxos log.
//!
//! A coordinator proposes into the next locally committed slot, durably learns
//! the result, and retries its requested value in a later slot if another
//! proposer won the current slot. Callers must serialize submissions per
//! process (for example, by holding a mutex around this coordinator and store).

use super::{
    membership_state::MembershipQuorum,
    paxos::{Ballot, ChosenValue, PaxosSlot},
    paxos_round::{PaxosRoundDriver, PaxosRoundError},
    paxos_storage::{PaxosStorageError, SqlitePaxosStore},
    peer::{DurablePaxosPeer, DurablePeerError},
    transport::PaxosRpc,
    weighted_quorum::VoterId,
};

/// Failures while submitting one value into the ordered Paxos log.
#[derive(Debug)]
pub enum PaxosLogError {
    InvalidAttemptLimit,
    ProposerIdentityMismatch {
        coordinator: VoterId,
        peer: VoterId,
    },
    Storage(PaxosStorageError),
    Peer(DurablePeerError),
    Round(PaxosRoundError),
    LearnQuorumUnavailable {
        chosen: ChosenValue,
        learned_voters: std::collections::BTreeSet<VoterId>,
    },
    LocalCommitDidNotAdvance {
        slot: PaxosSlot,
        commit_index: PaxosSlot,
    },
    SlotOverflow,
    AttemptsExhausted {
        last_slot: PaxosSlot,
    },
}

impl std::fmt::Display for PaxosLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAttemptLimit => {
                write!(f, "Paxos submission attempt limit must be positive")
            }
            Self::ProposerIdentityMismatch { coordinator, peer } => write!(
                f,
                "coordinator voter {} does not match local acceptor {}",
                coordinator.as_str(),
                peer.as_str()
            ),
            Self::Storage(error) => write!(f, "Paxos log storage failed: {error}"),
            Self::Peer(error) => write!(f, "local Paxos learn failed: {error}"),
            Self::Round(error) => write!(f, "Paxos log round failed: {error}"),
            Self::LearnQuorumUnavailable {
                chosen,
                learned_voters,
            } => write!(
                f,
                "slot {} chose a value but Learn quorum was not reached ({} voters learned)",
                chosen.slot,
                learned_voters.len()
            ),
            Self::LocalCommitDidNotAdvance { slot, commit_index } => write!(
                f,
                "local commit index {commit_index} did not reach chosen slot {slot}"
            ),
            Self::SlotOverflow => write!(f, "Paxos log slot exceeds u64"),
            Self::AttemptsExhausted { last_slot } => write!(
                f,
                "Paxos submission attempt limit exhausted at slot {last_slot}"
            ),
        }
    }
}

impl std::error::Error for PaxosLogError {}

/// Serial, bounded client-operation coordinator for one node's Paxos log.
pub struct PaxosLogCoordinator {
    proposer: VoterId,
    driver: PaxosRoundDriver,
    max_attempts: usize,
}

impl PaxosLogCoordinator {
    /// Creates a coordinator. Each retry uses a new, durably allocated ballot.
    pub fn new(
        proposer: VoterId,
        driver: PaxosRoundDriver,
        max_attempts: usize,
    ) -> Result<Self, PaxosLogError> {
        if max_attempts == 0 {
            return Err(PaxosLogError::InvalidAttemptLimit);
        }
        Ok(Self {
            proposer,
            driver,
            max_attempts,
        })
    }

    /// Submits `value`, returning only after its value is durably learned locally
    /// and Learn acknowledgements establish a quorum. If a different value is
    /// chosen in a slot, that value is learned and this operation retries in
    /// the next slot. A preempted ballot retries the same slot above the
    /// observed round.
    ///
    /// Client idempotency across process restarts is the caller's
    /// responsibility; encode an operation ID in `value` if retries must not
    /// execute an application command twice.
    pub fn submit(
        &mut self,
        value: Vec<u8>,
        membership: MembershipQuorum,
        peer: &mut DurablePaxosPeer,
        store: &mut SqlitePaxosStore,
    ) -> Result<ChosenValue, PaxosLogError> {
        if peer.node_id() != &self.proposer {
            return Err(PaxosLogError::ProposerIdentityMismatch {
                coordinator: self.proposer.clone(),
                peer: peer.node_id().clone(),
            });
        }

        let mut slot = store
            .commit_index()
            .map_err(PaxosLogError::Storage)?
            .checked_add(1)
            .ok_or(PaxosLogError::SlotOverflow)?;
        let mut observed_round = 0;

        for attempt in 0..self.max_attempts {
            let round = store
                .next_ballot_round_after(&self.proposer, observed_round)
                .map_err(PaxosLogError::Storage)?;
            let ballot = Ballot::new(round, self.proposer.clone());
            let outcome =
                match self
                    .driver
                    .run_round(slot, ballot, value.clone(), membership.clone())
                {
                    Ok(outcome) => outcome,
                    Err(PaxosRoundError::Preempted(ballot)) => {
                        observed_round = ballot.round();
                        if attempt + 1 == self.max_attempts {
                            return Err(PaxosLogError::AttemptsExhausted { last_slot: slot });
                        }
                        continue;
                    }
                    Err(error) => return Err(PaxosLogError::Round(error)),
                };

            let chosen = outcome.chosen;
            peer.handle_rpc(PaxosRpc::Learn(chosen.clone()), &membership, store)
                .map_err(PaxosLogError::Peer)?;
            let commit_index = store.commit_index().map_err(PaxosLogError::Storage)?;
            if commit_index < chosen.slot {
                return Err(PaxosLogError::LocalCommitDidNotAdvance {
                    slot: chosen.slot,
                    commit_index,
                });
            }
            if !outcome.learned_quorum {
                return Err(PaxosLogError::LearnQuorumUnavailable {
                    chosen,
                    learned_voters: outcome.learned_voters,
                });
            }
            if chosen.value == value {
                return Ok(chosen);
            }

            slot = commit_index
                .checked_add(1)
                .ok_or(PaxosLogError::SlotOverflow)?;
            observed_round = 0;
            if attempt + 1 == self.max_attempts {
                return Err(PaxosLogError::AttemptsExhausted { last_slot: slot });
            }
        }

        Err(PaxosLogError::AttemptsExhausted { last_slot: slot })
    }
}
