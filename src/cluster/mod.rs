//! Cluster membership and consensus-related types.

pub mod application;
pub mod membership_commit;
pub mod membership_state;
pub mod paxos;
pub mod paxos_log;
pub mod paxos_round;
pub mod paxos_storage;
pub mod peer;
pub mod transport;
pub mod weighted_quorum;
