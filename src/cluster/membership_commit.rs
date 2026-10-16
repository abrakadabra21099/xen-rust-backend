//! Weighted commit tracking for membership records.
//!
//! This is a deterministic building block for a consensus engine. It does not
//! elect leaders, replicate records, persist state, or provide network safety
//! on its own.

use std::collections::{BTreeMap, BTreeSet};

use super::membership_state::{
    MembershipLogEntry, MembershipQuorum, MembershipState, MembershipStateError,
    MembershipStateMachine,
};
use super::weighted_quorum::{QuorumConfigError, VoterId, WeightedMembership};

/// Position of a membership record in the local log.
pub type MembershipLogIndex = u64;

/// A membership record and the acknowledgements received for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipRecord {
    index: MembershipLogIndex,
    entry: MembershipLogEntry,
    quorum: MembershipQuorum,
    acknowledgements: BTreeSet<VoterId>,
}

impl MembershipRecord {
    pub fn index(&self) -> MembershipLogIndex {
        self.index
    }

    pub fn entry(&self) -> &MembershipLogEntry {
        &self.entry
    }

    pub fn acknowledgements(&self) -> &BTreeSet<VoterId> {
        &self.acknowledgements
    }

    pub fn has_quorum(&self) -> Result<bool, QuorumConfigError> {
        self.quorum
            .has_quorum(self.acknowledgements.iter().cloned())
    }
}

/// Errors returned by [`MembershipCommitTracker`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipCommitError {
    PendingRecord,
    UnknownRecord(MembershipLogIndex),
    IndexOverflow,
    InvalidVoter(QuorumConfigError),
    InvalidTransition(MembershipStateError),
    InvalidAcknowledgement(QuorumConfigError),
}

impl std::fmt::Display for MembershipCommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PendingRecord => write!(f, "the previous membership record is not committed"),
            Self::UnknownRecord(index) => write!(f, "unknown membership record at index {index}"),
            Self::IndexOverflow => write!(f, "membership log index exceeds u64"),
            Self::InvalidVoter(error) => write!(f, "invalid voter acknowledgement: {error}"),
            Self::InvalidTransition(error) => write!(f, "invalid membership transition: {error}"),
            Self::InvalidAcknowledgement(error) => {
                write!(f, "invalid voter acknowledgement: {error}")
            }
        }
    }
}

impl std::error::Error for MembershipCommitError {}

/// Tracks membership-only log records until they reach their required quorum.
///
/// Records are committed and applied strictly in index order. At most one
/// membership record may be pending at a time so a later configuration cannot
/// be calculated from a transition that has not committed yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipCommitTracker {
    state_machine: MembershipStateMachine,
    records: BTreeMap<MembershipLogIndex, MembershipRecord>,
    last_index: MembershipLogIndex,
    commit_index: MembershipLogIndex,
}

impl MembershipCommitTracker {
    pub fn new(initial: WeightedMembership) -> Self {
        Self {
            state_machine: MembershipStateMachine::new(initial),
            records: BTreeMap::new(),
            last_index: 0,
            commit_index: 0,
        }
    }

    pub fn state(&self) -> &MembershipState {
        self.state_machine.state()
    }

    pub fn last_index(&self) -> MembershipLogIndex {
        self.last_index
    }

    pub fn commit_index(&self) -> MembershipLogIndex {
        self.commit_index
    }

    pub fn record(&self, index: MembershipLogIndex) -> Option<&MembershipRecord> {
        self.records.get(&index)
    }

    /// Appends a membership transition using the quorum rule for its phase.
    pub fn append(
        &mut self,
        entry: MembershipLogEntry,
    ) -> Result<MembershipLogIndex, MembershipCommitError> {
        if self.commit_index != self.last_index {
            return Err(MembershipCommitError::PendingRecord);
        }

        let quorum = self
            .state_machine
            .quorum_for(&entry)
            .map_err(MembershipCommitError::InvalidTransition)?;
        let index = self
            .last_index
            .checked_add(1)
            .ok_or(MembershipCommitError::IndexOverflow)?;

        self.records.insert(
            index,
            MembershipRecord {
                index,
                entry,
                quorum,
                acknowledgements: BTreeSet::new(),
            },
        );
        self.last_index = index;
        Ok(index)
    }

    /// Records one voter acknowledgement and commits any newly ready prefix.
    ///
    /// The returned indexes are committed in order. Duplicate acknowledgements
    /// are idempotent and count once. The caller remains responsible for
    /// authenticating who sent the acknowledgement and for persisting data
    /// before sending protocol replies.
    pub fn acknowledge(
        &mut self,
        index: MembershipLogIndex,
        voter: VoterId,
    ) -> Result<Vec<MembershipLogIndex>, MembershipCommitError> {
        let record = self
            .records
            .get_mut(&index)
            .ok_or(MembershipCommitError::UnknownRecord(index))?;

        // Validate membership even when this acknowledgement alone cannot
        // satisfy the threshold. Unknown workers/stale voters never count.
        record
            .quorum
            .has_quorum([voter.clone()])
            .map_err(MembershipCommitError::InvalidVoter)?;
        record.acknowledgements.insert(voter);

        self.commit_ready_prefix()
    }

    fn commit_ready_prefix(&mut self) -> Result<Vec<MembershipLogIndex>, MembershipCommitError> {
        let mut committed = Vec::new();
        while self.commit_index < self.last_index {
            let next = self.commit_index + 1;
            let record = self
                .records
                .get(&next)
                .expect("every appended membership index is present");
            if !record
                .has_quorum()
                .map_err(MembershipCommitError::InvalidAcknowledgement)?
            {
                break;
            }

            let entry = record.entry.clone();
            self.state_machine
                .apply_committed(entry)
                .map_err(MembershipCommitError::InvalidTransition)?;
            self.commit_index = next;
            committed.push(next);
        }
        Ok(committed)
    }
}
