//! Deterministic state machine for committed weighted-membership log entries.

use super::weighted_quorum::{
    JointWeightedMembership, MembershipChange, QuorumConfigError, VoterId, WeightedMembership,
};

/// Entries that a consensus engine can append to its replicated log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipLogEntry {
    /// Start an explicit add, weight update, or removal transition.
    EnterJoint(MembershipChange),
    /// Finish a transition after the joint configuration has committed.
    FinalizeJoint,
}

/// The quorum rule to use when evaluating a log entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipQuorum {
    Stable(WeightedMembership),
    Joint(JointWeightedMembership),
}

impl MembershipQuorum {
    /// Returns whether the voter belongs to at least one side of this quorum.
    pub fn contains_voter(&self, voter: &VoterId) -> bool {
        match self {
            Self::Stable(membership) => membership.weight_of(voter).is_some(),
            Self::Joint(membership) => {
                membership.old().weight_of(voter).is_some()
                    || membership.new_membership().weight_of(voter).is_some()
            }
        }
    }

    /// Checks acknowledgements under the quorum rule represented by this value.
    pub fn has_quorum<I>(&self, acknowledgers: I) -> Result<bool, QuorumConfigError>
    where
        I: IntoIterator<Item = VoterId>,
    {
        match self {
            Self::Stable(membership) => membership.has_quorum(acknowledgers),
            Self::Joint(membership) => membership.has_quorum(acknowledgers),
        }
    }
}

/// Current applied membership state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipState {
    Stable(WeightedMembership),
    Joint(JointWeightedMembership),
}

/// Errors from invalid committed membership log entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStateError {
    TransitionAlreadyInProgress,
    NoTransitionInProgress,
    InvalidChange(QuorumConfigError),
}

impl std::fmt::Display for MembershipStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransitionAlreadyInProgress => {
                write!(f, "a membership transition is already in progress")
            }
            Self::NoTransitionInProgress => {
                write!(f, "there is no membership transition to finalize")
            }
            Self::InvalidChange(error) => write!(f, "invalid membership change: {error}"),
        }
    }
}

impl std::error::Error for MembershipStateError {}

/// Deterministically applies committed membership entries and exposes the
/// quorum each entry must satisfy before it may be committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipStateMachine {
    state: MembershipState,
}

impl MembershipStateMachine {
    /// Creates a state machine with the initial stable membership.
    pub fn new(initial: WeightedMembership) -> Self {
        Self {
            state: MembershipState::Stable(initial),
        }
    }

    /// Returns the current applied membership state.
    pub fn state(&self) -> &MembershipState {
        &self.state
    }

    /// Returns the quorum rule required to commit the supplied entry.
    ///
    /// Both entering and leaving joint mode require acknowledgements that form
    /// a quorum under the old and new weighted configurations. This is
    /// deliberately conservative and keeps the quorum rule consistent through
    /// the whole transition.
    pub fn quorum_for(
        &self,
        entry: &MembershipLogEntry,
    ) -> Result<MembershipQuorum, MembershipStateError> {
        match (&self.state, entry) {
            (MembershipState::Stable(current), MembershipLogEntry::EnterJoint(change)) => {
                let joint = current
                    .begin_change(change.clone())
                    .map_err(MembershipStateError::InvalidChange)?;
                Ok(MembershipQuorum::Joint(joint))
            }
            (MembershipState::Joint(joint), MembershipLogEntry::FinalizeJoint) => {
                Ok(MembershipQuorum::Joint(joint.clone()))
            }
            (MembershipState::Stable(_), MembershipLogEntry::FinalizeJoint) => {
                Err(MembershipStateError::NoTransitionInProgress)
            }
            (MembershipState::Joint(_), MembershipLogEntry::EnterJoint(_)) => {
                Err(MembershipStateError::TransitionAlreadyInProgress)
            }
        }
    }

    /// Applies an entry that the consensus engine has already committed.
    pub fn apply_committed(
        &mut self,
        entry: MembershipLogEntry,
    ) -> Result<(), MembershipStateError> {
        match (&self.state, entry) {
            (MembershipState::Stable(current), MembershipLogEntry::EnterJoint(change)) => {
                let joint = current
                    .begin_change(change)
                    .map_err(MembershipStateError::InvalidChange)?;
                self.state = MembershipState::Joint(joint);
                Ok(())
            }
            (MembershipState::Joint(joint), MembershipLogEntry::FinalizeJoint) => {
                self.state = MembershipState::Stable(joint.new_membership().clone());
                Ok(())
            }
            (MembershipState::Stable(_), MembershipLogEntry::FinalizeJoint) => {
                Err(MembershipStateError::NoTransitionInProgress)
            }
            (MembershipState::Joint(_), MembershipLogEntry::EnterJoint(_)) => {
                Err(MembershipStateError::TransitionAlreadyInProgress)
            }
        }
    }
}
