//! Durable request handling for inbound Paxos peer RPCs.

use super::{
    membership_state::MembershipQuorum,
    paxos::WeightedAcceptor,
    paxos_storage::{PaxosStorageError, SqlitePaxosStore},
    transport::PaxosRpc,
    weighted_quorum::VoterId,
};

/// Errors returned while handling an inbound durable Paxos message.
#[derive(Debug)]
pub enum DurablePeerError {
    Storage(PaxosStorageError),
    UnexpectedMessageDirection,
}

impl std::fmt::Display for DurablePeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "durable Paxos RPC failed: {error}"),
            Self::UnexpectedMessageDirection => {
                write!(
                    f,
                    "inbound Paxos RPC must be a prepare, accept, or learn request"
                )
            }
        }
    }
}

impl std::error::Error for DurablePeerError {}

impl From<PaxosStorageError> for DurablePeerError {
    fn from(error: PaxosStorageError) -> Self {
        Self::Storage(error)
    }
}

/// One control-plane node's durable Paxos acceptor.
///
/// The current membership is supplied for each request so an explicit,
/// committed membership transition can change the quorum rule without
/// reconstructing the local acceptor state.
pub struct DurablePaxosPeer {
    node_id: VoterId,
    acceptor: WeightedAcceptor,
}

impl DurablePaxosPeer {
    /// Creates an acceptor for a new node with no in-memory slot state.
    pub fn new(node_id: VoterId) -> Self {
        Self {
            acceptor: WeightedAcceptor::new(node_id.clone()),
            node_id,
        }
    }

    /// Restores durable acceptor state before the node rejoins the voting set.
    pub fn restore(node_id: VoterId, store: &SqlitePaxosStore) -> Result<Self, PaxosStorageError> {
        Ok(Self {
            acceptor: WeightedAcceptor::restore(node_id.clone(), store)?,
            node_id,
        })
    }

    /// Validates and durably handles one inbound request before replying.
    pub fn handle_rpc(
        &mut self,
        request: PaxosRpc,
        membership: &MembershipQuorum,
        store: &mut SqlitePaxosStore,
    ) -> Result<PaxosRpc, DurablePeerError> {
        match request {
            PaxosRpc::Prepare(request) => Ok(PaxosRpc::Promise(
                self.acceptor
                    .handle_prepare_durable(request, membership, store)?,
            )),
            PaxosRpc::Accept(request) => Ok(PaxosRpc::Accepted(
                self.acceptor
                    .handle_accept_durable(request, membership, store)?,
            )),
            PaxosRpc::Learn(chosen) => {
                let slot = chosen.slot;
                self.acceptor
                    .handle_learn_durable(chosen, membership, store)?;
                Ok(PaxosRpc::Learned {
                    slot,
                    voter: self.node_id.clone(),
                })
            }
            PaxosRpc::Promise(_) | PaxosRpc::Accepted(_) | PaxosRpc::Learned { .. } => {
                Err(DurablePeerError::UnexpectedMessageDirection)
            }
        }
    }
}
