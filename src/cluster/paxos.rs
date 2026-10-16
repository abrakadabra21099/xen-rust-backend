//! Leaderless weighted Paxos round for one replicated-log slot.
//!
//! Every control-plane voter may create a proposer for a client request. This
//! module provides prepare/accept state transitions and an optional SQLite
//! durable acceptor path; it does not provide transport, retries, or multi-slot
//! log management.

use std::collections::{BTreeMap, BTreeSet};

use super::membership_state::MembershipQuorum;
use super::paxos_storage::{PaxosStorageError, SqlitePaxosStore};
use super::weighted_quorum::{QuorumConfigError, VoterId};

/// A globally ordered proposal identifier. The node ID breaks ties between
/// concurrent proposers using the same round number.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Ballot {
    round: u64,
    proposer: VoterId,
}

impl Ballot {
    pub fn new(round: u64, proposer: VoterId) -> Self {
        Self { round, proposer }
    }

    pub fn round(&self) -> u64 {
        self.round
    }

    pub fn proposer(&self) -> &VoterId {
        &self.proposer
    }
}

/// One-based replicated log slot. Slots are agreed independently; callers must
/// only activate later slots after all required earlier slots have been chosen.
pub type PaxosSlot = u64;

/// A value previously accepted by an acceptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedValue {
    pub ballot: Ballot,
    pub value: Vec<u8>,
}

/// Proof that a value was accepted by a weighted quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChosenValue {
    pub slot: PaxosSlot,
    pub ballot: Ballot,
    pub value: Vec<u8>,
    pub acceptors: BTreeSet<VoterId>,
}

/// Prepare request broadcast by any eligible proposer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareRequest {
    pub slot: PaxosSlot,
    pub ballot: Ballot,
}

/// Prepare response from one acceptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromiseResponse {
    Promised {
        slot: PaxosSlot,
        ballot: Ballot,
        voter: VoterId,
        accepted: Option<AcceptedValue>,
    },
    Rejected {
        slot: PaxosSlot,
        ballot: Ballot,
        voter: VoterId,
        promised: Ballot,
    },
}

/// Accept request broadcast after a prepare quorum has been collected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptRequest {
    pub slot: PaxosSlot,
    pub ballot: Ballot,
    pub value: Vec<u8>,
}

/// Accept response from one acceptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptedResponse {
    Accepted {
        slot: PaxosSlot,
        ballot: Ballot,
        voter: VoterId,
    },
    Rejected {
        slot: PaxosSlot,
        ballot: Ballot,
        voter: VoterId,
        promised: Ballot,
    },
}

/// Acceptor response that a proposer should send next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromiseProgress {
    Ignored,
    Pending,
    SendAccept(AcceptRequest),
    Preempted(Ballot),
}

/// Proposer response after receiving an accept acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptProgress {
    Ignored,
    Pending,
    Chosen(ChosenValue),
    Preempted(Ballot),
}

/// Protocol errors for malformed or inconsistent Paxos messages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaxosError {
    InvalidSlot,
    NotVoter(VoterId),
    InvalidVoter(QuorumConfigError),
    WrongSlot {
        expected: PaxosSlot,
        actual: PaxosSlot,
    },
    WrongBallot,
    ConflictingDuplicate(VoterId),
    ConflictingAcceptedValue(Ballot),
    ConflictingChosenValue(PaxosSlot),
    InsufficientQuorum,
}

impl std::fmt::Display for PaxosError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSlot => write!(f, "Paxos log slots are one-based"),
            Self::NotVoter(id) => write!(f, "node {} is not a voter", id.as_str()),
            Self::InvalidVoter(error) => write!(f, "invalid voter: {error}"),
            Self::WrongSlot { expected, actual } => {
                write!(f, "message for slot {actual}; expected slot {expected}")
            }
            Self::WrongBallot => write!(f, "message ballot does not match this proposal"),
            Self::ConflictingDuplicate(id) => {
                write!(f, "conflicting duplicate response from {}", id.as_str())
            }
            Self::ConflictingAcceptedValue(ballot) => write!(
                f,
                "acceptors report different values for ballot {} from {}",
                ballot.round(),
                ballot.proposer().as_str()
            ),
            Self::ConflictingChosenValue(slot) => {
                write!(f, "conflicting chosen values for slot {slot}")
            }
            Self::InsufficientQuorum => write!(f, "acceptor set does not form a quorum"),
        }
    }
}

impl std::error::Error for PaxosError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AcceptorSlot {
    pub(crate) promised: Option<Ballot>,
    pub(crate) accepted: Option<AcceptedValue>,
    pub(crate) chosen: Option<ChosenValue>,
}

/// In-memory acceptor state for slots handled by one control-plane node.
///
/// The in-memory methods are useful for protocol simulation. Production code
/// should use the `_durable` methods and restore state from the node-local store
/// before the acceptor rejoins the voting set after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedAcceptor {
    node_id: VoterId,
    slots: BTreeMap<PaxosSlot, AcceptorSlot>,
}

impl WeightedAcceptor {
    pub fn new(node_id: VoterId) -> Self {
        Self {
            node_id,
            slots: BTreeMap::new(),
        }
    }

    /// Restores the acceptor's in-memory view from its node-local SQLite file.
    pub fn restore(node_id: VoterId, store: &SqlitePaxosStore) -> Result<Self, PaxosStorageError> {
        Ok(Self {
            node_id,
            slots: store.load_acceptor_slots()?,
        })
    }

    pub fn handle_prepare(
        &mut self,
        request: PrepareRequest,
        membership: &MembershipQuorum,
    ) -> Result<PromiseResponse, PaxosError> {
        validate_slot(request.slot)?;
        self.ensure_eligible(&request.ballot, membership)?;
        let state = self.slots.entry(request.slot).or_default();
        if state
            .promised
            .as_ref()
            .is_some_and(|promised| request.ballot < *promised)
        {
            return Ok(PromiseResponse::Rejected {
                slot: request.slot,
                ballot: request.ballot,
                voter: self.node_id.clone(),
                promised: state.promised.clone().expect("promise checked above"),
            });
        }

        state.promised = Some(request.ballot.clone());
        Ok(PromiseResponse::Promised {
            slot: request.slot,
            ballot: request.ballot,
            voter: self.node_id.clone(),
            accepted: state.accepted.clone(),
        })
    }

    /// Durable prepare path: commits the promise before returning a grant.
    pub fn handle_prepare_durable(
        &mut self,
        request: PrepareRequest,
        membership: &MembershipQuorum,
        store: &mut SqlitePaxosStore,
    ) -> Result<PromiseResponse, PaxosStorageError> {
        validate_slot(request.slot)?;
        self.ensure_eligible(&request.ballot, membership)?;
        let slot = request.slot;
        let node_id = self.node_id.clone();
        let (response, state) = store.update_slot(slot, |state| {
            if state
                .promised
                .as_ref()
                .is_some_and(|promised| request.ballot < *promised)
            {
                return Ok(PromiseResponse::Rejected {
                    slot,
                    ballot: request.ballot.clone(),
                    voter: node_id.clone(),
                    promised: state.promised.clone().expect("promise checked above"),
                });
            }
            state.promised = Some(request.ballot.clone());
            Ok(PromiseResponse::Promised {
                slot,
                ballot: request.ballot.clone(),
                voter: node_id.clone(),
                accepted: state.accepted.clone(),
            })
        })?;
        self.slots.insert(slot, state);
        Ok(response)
    }

    pub fn handle_accept(
        &mut self,
        request: AcceptRequest,
        membership: &MembershipQuorum,
    ) -> Result<AcceptedResponse, PaxosError> {
        validate_slot(request.slot)?;
        self.ensure_eligible(&request.ballot, membership)?;
        let state = self.slots.entry(request.slot).or_default();
        if state
            .promised
            .as_ref()
            .is_some_and(|promised| request.ballot < *promised)
        {
            return Ok(AcceptedResponse::Rejected {
                slot: request.slot,
                ballot: request.ballot,
                voter: self.node_id.clone(),
                promised: state.promised.clone().expect("promise checked above"),
            });
        }

        if let Some(accepted) = &state.accepted {
            if accepted.ballot == request.ballot && accepted.value != request.value {
                return Err(PaxosError::ConflictingAcceptedValue(request.ballot));
            }
        }

        state.promised = Some(request.ballot.clone());
        state.accepted = Some(AcceptedValue {
            ballot: request.ballot.clone(),
            value: request.value,
        });
        Ok(AcceptedResponse::Accepted {
            slot: request.slot,
            ballot: request.ballot,
            voter: self.node_id.clone(),
        })
    }

    /// Durable accept path: commits the accepted value before returning success.
    pub fn handle_accept_durable(
        &mut self,
        request: AcceptRequest,
        membership: &MembershipQuorum,
        store: &mut SqlitePaxosStore,
    ) -> Result<AcceptedResponse, PaxosStorageError> {
        validate_slot(request.slot)?;
        self.ensure_eligible(&request.ballot, membership)?;
        let slot = request.slot;
        let node_id = self.node_id.clone();
        let (response, state) = store.update_slot(slot, |state| {
            if state
                .promised
                .as_ref()
                .is_some_and(|promised| request.ballot < *promised)
            {
                return Ok(AcceptedResponse::Rejected {
                    slot,
                    ballot: request.ballot.clone(),
                    voter: node_id.clone(),
                    promised: state.promised.clone().expect("promise checked above"),
                });
            }
            if let Some(accepted) = &state.accepted {
                if accepted.ballot == request.ballot && accepted.value != request.value {
                    return Err(PaxosError::ConflictingAcceptedValue(request.ballot.clone()));
                }
            }
            state.promised = Some(request.ballot.clone());
            state.accepted = Some(AcceptedValue {
                ballot: request.ballot.clone(),
                value: request.value.clone(),
            });
            Ok(AcceptedResponse::Accepted {
                slot,
                ballot: request.ballot.clone(),
                voter: node_id.clone(),
            })
        })?;
        self.slots.insert(slot, state);
        Ok(response)
    }

    pub fn accepted(&self, slot: PaxosSlot) -> Option<&AcceptedValue> {
        self.slots
            .get(&slot)
            .and_then(|state| state.accepted.as_ref())
    }

    /// Learns a value only when the attached acceptor set forms the quorum.
    pub fn handle_learn(
        &mut self,
        chosen: ChosenValue,
        membership: &MembershipQuorum,
    ) -> Result<(), PaxosError> {
        validate_slot(chosen.slot)?;
        if !membership.contains_voter(&self.node_id) {
            return Err(PaxosError::NotVoter(self.node_id.clone()));
        }
        if !membership.contains_voter(&chosen.ballot.proposer) {
            return Err(PaxosError::InvalidVoter(QuorumConfigError::UnknownVoter(
                chosen.ballot.proposer.clone(),
            )));
        }
        if !membership
            .has_quorum(chosen.acceptors.iter().cloned())
            .map_err(PaxosError::InvalidVoter)?
        {
            return Err(PaxosError::InsufficientQuorum);
        }

        let state = self.slots.entry(chosen.slot).or_default();
        if state.chosen.as_ref().is_some_and(|existing| {
            existing.value != chosen.value || existing.ballot != chosen.ballot
        }) {
            return Err(PaxosError::ConflictingChosenValue(chosen.slot));
        }
        if state.accepted.as_ref().is_some_and(|accepted| {
            accepted.ballot > chosen.ballot && accepted.value != chosen.value
        }) {
            return Err(PaxosError::ConflictingChosenValue(chosen.slot));
        }
        state.chosen = Some(chosen);
        Ok(())
    }

    /// Durable learn path: stores the chosen record before it can be applied.
    pub fn handle_learn_durable(
        &mut self,
        chosen: ChosenValue,
        membership: &MembershipQuorum,
        store: &mut SqlitePaxosStore,
    ) -> Result<(), PaxosStorageError> {
        validate_slot(chosen.slot)?;
        if !membership.contains_voter(&self.node_id) {
            return Err(PaxosError::NotVoter(self.node_id.clone()).into());
        }
        if !membership.contains_voter(&chosen.ballot.proposer) {
            return Err(PaxosError::InvalidVoter(QuorumConfigError::UnknownVoter(
                chosen.ballot.proposer.clone(),
            ))
            .into());
        }
        if !membership
            .has_quorum(chosen.acceptors.iter().cloned())
            .map_err(PaxosError::InvalidVoter)?
        {
            return Err(PaxosError::InsufficientQuorum.into());
        }

        let slot = chosen.slot;
        let ((), state) = store.update_slot(slot, |state| {
            if state.chosen.as_ref().is_some_and(|existing| {
                existing.value != chosen.value || existing.ballot != chosen.ballot
            }) {
                return Err(PaxosError::ConflictingChosenValue(slot));
            }
            if state.accepted.as_ref().is_some_and(|accepted| {
                accepted.ballot > chosen.ballot && accepted.value != chosen.value
            }) {
                return Err(PaxosError::ConflictingChosenValue(slot));
            }
            state.chosen = Some(chosen.clone());
            Ok(())
        })?;
        self.slots.insert(slot, state);
        Ok(())
    }

    pub fn chosen(&self, slot: PaxosSlot) -> Option<&ChosenValue> {
        self.slots
            .get(&slot)
            .and_then(|state| state.chosen.as_ref())
    }

    fn ensure_eligible(
        &self,
        ballot: &Ballot,
        membership: &MembershipQuorum,
    ) -> Result<(), PaxosError> {
        if !membership.contains_voter(&self.node_id) {
            return Err(PaxosError::NotVoter(self.node_id.clone()));
        }
        if !membership.contains_voter(&ballot.proposer) {
            return Err(PaxosError::InvalidVoter(QuorumConfigError::UnknownVoter(
                ballot.proposer.clone(),
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProposerPhase {
    Preparing,
    Accepting(Vec<u8>),
    Chosen(ChosenValue),
    Preempted(Ballot),
}

/// One concurrent proposer for a slot. Multiple nodes may run this at once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedProposer {
    slot: PaxosSlot,
    ballot: Ballot,
    membership: MembershipQuorum,
    requested_value: Vec<u8>,
    promises: BTreeMap<VoterId, Option<AcceptedValue>>,
    acceptances: BTreeSet<VoterId>,
    phase: ProposerPhase,
}

impl WeightedProposer {
    pub fn new(
        slot: PaxosSlot,
        ballot: Ballot,
        requested_value: Vec<u8>,
        membership: MembershipQuorum,
    ) -> Result<Self, PaxosError> {
        validate_slot(slot)?;
        if !membership.contains_voter(&ballot.proposer) {
            return Err(PaxosError::InvalidVoter(QuorumConfigError::UnknownVoter(
                ballot.proposer.clone(),
            )));
        }
        Ok(Self {
            slot,
            ballot,
            membership,
            requested_value,
            promises: BTreeMap::new(),
            acceptances: BTreeSet::new(),
            phase: ProposerPhase::Preparing,
        })
    }

    pub fn prepare_request(&self) -> PrepareRequest {
        PrepareRequest {
            slot: self.slot,
            ballot: self.ballot.clone(),
        }
    }

    pub fn handle_promise(
        &mut self,
        response: PromiseResponse,
    ) -> Result<PromiseProgress, PaxosError> {
        match response {
            PromiseResponse::Rejected {
                slot,
                ballot,
                voter,
                promised,
            } => {
                self.validate_response(slot, &ballot, &voter)?;
                if promised > self.ballot {
                    self.phase = ProposerPhase::Preempted(promised.clone());
                    Ok(PromiseProgress::Preempted(promised))
                } else {
                    Ok(PromiseProgress::Ignored)
                }
            }
            PromiseResponse::Promised {
                slot,
                ballot,
                voter,
                accepted,
            } => {
                self.validate_response(slot, &ballot, &voter)?;
                if !matches!(self.phase, ProposerPhase::Preparing) {
                    return Ok(PromiseProgress::Ignored);
                }
                if let Some(previous) = self.promises.get(&voter) {
                    if previous != &accepted {
                        return Err(PaxosError::ConflictingDuplicate(voter));
                    }
                    return Ok(PromiseProgress::Pending);
                }
                self.promises.insert(voter, accepted);
                if !self
                    .membership
                    .has_quorum(self.promises.keys().cloned())
                    .map_err(PaxosError::InvalidVoter)?
                {
                    return Ok(PromiseProgress::Pending);
                }

                let value = self.value_from_promises()?;
                self.phase = ProposerPhase::Accepting(value.clone());
                Ok(PromiseProgress::SendAccept(AcceptRequest {
                    slot: self.slot,
                    ballot: self.ballot.clone(),
                    value,
                }))
            }
        }
    }

    pub fn handle_accepted(
        &mut self,
        response: AcceptedResponse,
    ) -> Result<AcceptProgress, PaxosError> {
        match response {
            AcceptedResponse::Rejected {
                slot,
                ballot,
                voter,
                promised,
            } => {
                self.validate_response(slot, &ballot, &voter)?;
                if promised > self.ballot {
                    self.phase = ProposerPhase::Preempted(promised.clone());
                    Ok(AcceptProgress::Preempted(promised))
                } else {
                    Ok(AcceptProgress::Ignored)
                }
            }
            AcceptedResponse::Accepted {
                slot,
                ballot,
                voter,
            } => {
                self.validate_response(slot, &ballot, &voter)?;
                let ProposerPhase::Accepting(value) = &self.phase else {
                    return match &self.phase {
                        ProposerPhase::Chosen(chosen) => Ok(AcceptProgress::Chosen(chosen.clone())),
                        _ => Ok(AcceptProgress::Ignored),
                    };
                };
                self.acceptances.insert(voter);
                if self
                    .membership
                    .has_quorum(self.acceptances.iter().cloned())
                    .map_err(PaxosError::InvalidVoter)?
                {
                    let chosen = ChosenValue {
                        slot: self.slot,
                        ballot: self.ballot.clone(),
                        value: value.clone(),
                        acceptors: self.acceptances.clone(),
                    };
                    self.phase = ProposerPhase::Chosen(chosen.clone());
                    Ok(AcceptProgress::Chosen(chosen))
                } else {
                    Ok(AcceptProgress::Pending)
                }
            }
        }
    }

    pub fn chosen_value(&self) -> Option<&ChosenValue> {
        match &self.phase {
            ProposerPhase::Chosen(chosen) => Some(chosen),
            _ => None,
        }
    }

    fn validate_response(
        &self,
        slot: PaxosSlot,
        ballot: &Ballot,
        voter: &VoterId,
    ) -> Result<(), PaxosError> {
        if slot != self.slot {
            return Err(PaxosError::WrongSlot {
                expected: self.slot,
                actual: slot,
            });
        }
        if ballot != &self.ballot {
            return Err(PaxosError::WrongBallot);
        }
        if !self.membership.contains_voter(voter) {
            return Err(PaxosError::InvalidVoter(QuorumConfigError::UnknownVoter(
                voter.clone(),
            )));
        }
        Ok(())
    }

    fn value_from_promises(&self) -> Result<Vec<u8>, PaxosError> {
        let highest = self
            .promises
            .values()
            .filter_map(|accepted| accepted.as_ref())
            .max_by(|left, right| left.ballot.cmp(&right.ballot));
        let Some(highest) = highest else {
            return Ok(self.requested_value.clone());
        };

        if self
            .promises
            .values()
            .filter_map(Option::as_ref)
            .any(|accepted| accepted.ballot == highest.ballot && accepted.value != highest.value)
        {
            return Err(PaxosError::ConflictingAcceptedValue(highest.ballot.clone()));
        }
        Ok(highest.value.clone())
    }
}

fn validate_slot(slot: PaxosSlot) -> Result<(), PaxosError> {
    if slot == 0 {
        Err(PaxosError::InvalidSlot)
    } else {
        Ok(())
    }
}
