//! Static weighted-majority quorum configuration.
//!
//! This module only calculates quorum membership. It is not a consensus
//! protocol: elections, durable log replication, membership changes, and
//! network transport must be provided by the consensus layer.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

/// Stable identity of a control-plane voter.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct VoterId(String);

impl VoterId {
    /// Creates an ID from a non-empty string.
    pub fn new(id: impl Into<String>) -> Result<Self, QuorumConfigError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(QuorumConfigError::EmptyVoterId);
        }
        Ok(Self(id))
    }

    /// Returns the underlying ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Immutable voter configuration for one consensus membership epoch.
///
/// Only control-plane voters belong here. Worker nodes are deliberately
/// represented elsewhere and cannot contribute quorum weight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedMembership {
    voters: BTreeMap<VoterId, NonZeroU64>,
    total_weight: u64,
}

impl WeightedMembership {
    /// Creates a membership from `(voter ID, positive weight)` pairs.
    pub fn new<I>(voters: I) -> Result<Self, QuorumConfigError>
    where
        I: IntoIterator<Item = (VoterId, u64)>,
    {
        let mut configured = BTreeMap::new();
        let mut total_weight = 0_u64;

        for (id, weight) in voters {
            let weight =
                NonZeroU64::new(weight).ok_or_else(|| QuorumConfigError::ZeroWeight(id.clone()))?;
            if configured.contains_key(&id) {
                return Err(QuorumConfigError::DuplicateVoter(id));
            }
            total_weight = total_weight
                .checked_add(weight.get())
                .ok_or(QuorumConfigError::TotalWeightOverflow)?;
            configured.insert(id, weight);
        }

        if configured.is_empty() {
            return Err(QuorumConfigError::EmptyMembership);
        }

        Ok(Self {
            voters: configured,
            total_weight,
        })
    }

    /// Returns the sum of all configured voter weights.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the strict weighted-majority threshold.
    ///
    /// A quorum must have more than half of the configured total weight.
    pub fn quorum_threshold(&self) -> u64 {
        self.total_weight / 2 + 1
    }

    /// Returns a voter's configured weight, if it belongs to this membership.
    pub fn weight_of(&self, voter: &VoterId) -> Option<u64> {
        self.voters.get(voter).map(|weight| weight.get())
    }

    /// Returns a new membership with an explicitly changed voter weight.
    ///
    /// This is a pure configuration operation. A running cluster must only
    /// activate the returned membership after an explicit API request has
    /// been committed through a safe consensus membership transition.
    pub fn with_updated_weight(
        &self,
        voter: &VoterId,
        new_weight: u64,
    ) -> Result<Self, QuorumConfigError> {
        let new_weight = NonZeroU64::new(new_weight)
            .ok_or_else(|| QuorumConfigError::ZeroWeight(voter.clone()))?;
        let old_weight = self
            .voters
            .get(voter)
            .ok_or_else(|| QuorumConfigError::UnknownVoter(voter.clone()))?;

        let total_weight = self
            .total_weight
            .checked_sub(old_weight.get())
            .and_then(|remaining| remaining.checked_add(new_weight.get()))
            .ok_or(QuorumConfigError::TotalWeightOverflow)?;
        let mut voters = self.voters.clone();
        voters.insert(voter.clone(), new_weight);

        Ok(Self {
            voters,
            total_weight,
        })
    }

    /// Returns the number of voting members.
    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    /// Builds a joint membership transition from an explicit cluster command.
    ///
    /// The returned configuration is only a proposal. A consensus engine must
    /// commit the joint configuration before activating its target membership.
    pub fn begin_change(
        &self,
        change: MembershipChange,
    ) -> Result<JointWeightedMembership, QuorumConfigError> {
        let target = match change {
            MembershipChange::AddVoter { id, weight } => {
                let weight = NonZeroU64::new(weight)
                    .ok_or_else(|| QuorumConfigError::ZeroWeight(id.clone()))?;
                if self.voters.contains_key(&id) {
                    return Err(QuorumConfigError::VoterAlreadyExists(id));
                }
                let total_weight = self
                    .total_weight
                    .checked_add(weight.get())
                    .ok_or(QuorumConfigError::TotalWeightOverflow)?;
                let mut voters = self.voters.clone();
                voters.insert(id, weight);
                Self {
                    voters,
                    total_weight,
                }
            }
            MembershipChange::UpdateWeight { id, new_weight } => {
                self.with_updated_weight(&id, new_weight)?
            }
            MembershipChange::RemoveVoter { id } => {
                let old_weight = self
                    .voters
                    .get(&id)
                    .ok_or_else(|| QuorumConfigError::UnknownVoter(id.clone()))?;
                if self.voters.len() == 1 {
                    return Err(QuorumConfigError::CannotRemoveLastVoter);
                }
                let mut voters = self.voters.clone();
                voters.remove(&id);
                Self {
                    voters,
                    total_weight: self.total_weight - old_weight.get(),
                }
            }
        };

        Ok(JointWeightedMembership::new(self.clone(), target))
    }

    /// Checks whether distinct acknowledgers meet the weighted-majority threshold.
    ///
    /// Unknown IDs are rejected so a worker or stale member cannot accidentally
    /// contribute weight. Repeated IDs count only once.
    pub fn has_quorum<I>(&self, acknowledgers: I) -> Result<bool, QuorumConfigError>
    where
        I: IntoIterator<Item = VoterId>,
    {
        let mut counted = BTreeSet::new();
        let mut acknowledged_weight = 0_u64;

        for id in acknowledgers {
            let Some(weight) = self.voters.get(&id) else {
                return Err(QuorumConfigError::UnknownVoter(id));
            };
            if counted.insert(id) {
                acknowledged_weight += weight.get();
            }
        }

        Ok(acknowledged_weight >= self.quorum_threshold())
    }
}

/// Explicit cluster API operations that propose a membership transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipChange {
    AddVoter { id: VoterId, weight: u64 },
    UpdateWeight { id: VoterId, new_weight: u64 },
    RemoveVoter { id: VoterId },
}

/// Joint quorum configuration used while transitioning between memberships.
///
/// An acknowledgement set must independently satisfy the weighted-majority
/// rule under both the old and new memberships before the transition can
/// advance. Voters present in only one membership count only toward that side.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JointWeightedMembership {
    old: WeightedMembership,
    new: WeightedMembership,
}

impl JointWeightedMembership {
    /// Creates a joint configuration for a consensus-approved transition.
    pub fn new(old: WeightedMembership, new: WeightedMembership) -> Self {
        Self { old, new }
    }

    /// Returns the previous membership.
    pub fn old(&self) -> &WeightedMembership {
        &self.old
    }

    /// Returns the target membership.
    pub fn new_membership(&self) -> &WeightedMembership {
        &self.new
    }

    /// Checks that acknowledgers form a quorum in both memberships.
    pub fn has_quorum<I>(&self, acknowledgers: I) -> Result<bool, QuorumConfigError>
    where
        I: IntoIterator<Item = VoterId>,
    {
        let acknowledgers: BTreeSet<_> = acknowledgers.into_iter().collect();
        for id in &acknowledgers {
            if self.old.weight_of(id).is_none() && self.new.weight_of(id).is_none() {
                return Err(QuorumConfigError::UnknownVoter(id.clone()));
            }
        }

        let old_votes = acknowledgers
            .iter()
            .filter(|id| self.old.weight_of(id).is_some())
            .cloned();
        let new_votes = acknowledgers
            .iter()
            .filter(|id| self.new.weight_of(id).is_some())
            .cloned();

        Ok(self.old.has_quorum(old_votes)? && self.new.has_quorum(new_votes)?)
    }
}

/// Invalid membership or quorum input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuorumConfigError {
    EmptyVoterId,
    EmptyMembership,
    DuplicateVoter(VoterId),
    VoterAlreadyExists(VoterId),
    ZeroWeight(VoterId),
    TotalWeightOverflow,
    UnknownVoter(VoterId),
    CannotRemoveLastVoter,
}

impl std::fmt::Display for QuorumConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyVoterId => write!(f, "voter ID must not be empty"),
            Self::EmptyMembership => write!(f, "membership must contain at least one voter"),
            Self::DuplicateVoter(id) => write!(f, "duplicate voter ID: {}", id.as_str()),
            Self::VoterAlreadyExists(id) => {
                write!(f, "voter already exists: {}", id.as_str())
            }
            Self::ZeroWeight(id) => write!(f, "voter {} must have a positive weight", id.as_str()),
            Self::TotalWeightOverflow => write!(f, "sum of voter weights exceeds u64"),
            Self::UnknownVoter(id) => write!(f, "unknown voter ID: {}", id.as_str()),
            Self::CannotRemoveLastVoter => write!(f, "membership must retain at least one voter"),
        }
    }
}

impl std::error::Error for QuorumConfigError {}
