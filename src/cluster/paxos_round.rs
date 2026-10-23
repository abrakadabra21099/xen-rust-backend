//! Leaderless, networked prepare/accept/learn round for one Paxos slot.

use std::{collections::BTreeSet, net::SocketAddr, sync::mpsc, thread, time::Duration};

use super::{
    membership_state::MembershipQuorum,
    paxos::{
        AcceptProgress, Ballot, ChosenValue, PaxosError, PaxosSlot, PromiseProgress,
        WeightedProposer,
    },
    transport::{tcp::TcpRpcClient, PaxosRpc},
    weighted_quorum::VoterId,
};

/// A configured network address for one control-plane voter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaxosPeerEndpoint {
    pub voter: VoterId,
    pub address: SocketAddr,
}

/// Which consensus phase failed to gather its required weighted quorum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaxosRoundPhase {
    Prepare,
    Accept,
}

/// Outcome of a one-slot proposal and best-effort Learn dissemination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaxosRoundOutcome {
    pub chosen: ChosenValue,
    pub learned_voters: BTreeSet<VoterId>,
    pub learned_quorum: bool,
}

/// Failures while coordinating a leaderless one-slot proposal.
#[derive(Debug)]
pub enum PaxosRoundError {
    DuplicatePeer(VoterId),
    InvalidProtocol(PaxosError),
    Preempted(Ballot),
    QuorumUnavailable(PaxosRoundPhase),
}

impl std::fmt::Display for PaxosRoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicatePeer(voter) => {
                write!(f, "duplicate endpoint for voter {}", voter.as_str())
            }
            Self::InvalidProtocol(error) => write!(f, "Paxos round rejected: {error}"),
            Self::Preempted(ballot) => write!(
                f,
                "Paxos proposal was preempted by ballot {} from {}",
                ballot.round(),
                ballot.proposer().as_str()
            ),
            Self::QuorumUnavailable(phase) => {
                write!(f, "weighted quorum unavailable during {phase:?}")
            }
        }
    }
}

impl std::error::Error for PaxosRoundError {}

/// Network driver for independently initiated Paxos rounds.
///
/// The driver never elects a cluster leader. Any voting node can run a round;
/// simultaneous proposers are resolved by ballot order and the underlying
/// acceptor promises.
pub struct PaxosRoundDriver {
    client: TcpRpcClient,
    peers: Vec<PaxosPeerEndpoint>,
}

impl PaxosRoundDriver {
    /// Creates a driver and rejects ambiguous duplicate voter endpoints.
    pub fn new(timeout: Duration, peers: Vec<PaxosPeerEndpoint>) -> Result<Self, PaxosRoundError> {
        let mut voters = BTreeSet::new();
        for peer in &peers {
            if !voters.insert(peer.voter.clone()) {
                return Err(PaxosRoundError::DuplicatePeer(peer.voter.clone()));
            }
        }
        Ok(Self {
            client: TcpRpcClient::new(timeout),
            peers,
        })
    }

    /// Runs prepare, accept, and learn dissemination for one slot.
    ///
    /// The caller must allocate `ballot` from its durable local ballot counter.
    /// A chosen value is returned after an accept quorum; `learned_quorum`
    /// reports whether the chosen certificate was also durably learned by a
    /// quorum. Learn dissemination is attempted even if some peers are down.
    pub fn run_round(
        &self,
        slot: PaxosSlot,
        ballot: Ballot,
        requested_value: Vec<u8>,
        membership: MembershipQuorum,
    ) -> Result<PaxosRoundOutcome, PaxosRoundError> {
        let mut proposer = WeightedProposer::new(slot, ballot, requested_value, membership.clone())
            .map_err(PaxosRoundError::InvalidProtocol)?;

        let prepare = PaxosRpc::Prepare(proposer.prepare_request());
        let mut accept_request = None;
        for (_, response) in self.broadcast(prepare, &membership) {
            let Ok(PaxosRpc::Promise(response)) = response else {
                continue;
            };
            match proposer
                .handle_promise(response)
                .map_err(PaxosRoundError::InvalidProtocol)?
            {
                PromiseProgress::SendAccept(request) => {
                    accept_request = Some(request);
                    break;
                }
                PromiseProgress::Preempted(ballot) => {
                    return Err(PaxosRoundError::Preempted(ballot));
                }
                PromiseProgress::Ignored | PromiseProgress::Pending => {}
            }
        }
        let accept_request =
            accept_request.ok_or(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Prepare))?;

        let mut chosen = None;
        for (_, response) in self.broadcast(PaxosRpc::Accept(accept_request), &membership) {
            let Ok(PaxosRpc::Accepted(response)) = response else {
                continue;
            };
            match proposer
                .handle_accepted(response)
                .map_err(PaxosRoundError::InvalidProtocol)?
            {
                AcceptProgress::Chosen(value) => {
                    chosen = Some(value);
                    break;
                }
                AcceptProgress::Preempted(ballot) => {
                    return Err(PaxosRoundError::Preempted(ballot));
                }
                AcceptProgress::Ignored | AcceptProgress::Pending => {}
            }
        }
        let chosen = chosen.ok_or(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Accept))?;

        let mut learned_voters = BTreeSet::new();
        let mut learned_quorum = false;
        for (peer, response) in self.broadcast(PaxosRpc::Learn(chosen.clone()), &membership) {
            if let Ok(PaxosRpc::Learned {
                slot: learned_slot,
                voter,
            }) = response
            {
                if learned_slot == chosen.slot && voter == peer {
                    learned_voters.insert(voter);
                    learned_quorum = membership
                        .has_quorum(learned_voters.iter().cloned())
                        .map_err(|error| {
                            PaxosRoundError::InvalidProtocol(PaxosError::InvalidVoter(error))
                        })?;
                    if learned_quorum {
                        break;
                    }
                }
            }
        }

        Ok(PaxosRoundOutcome {
            chosen,
            learned_voters,
            learned_quorum,
        })
    }

    fn broadcast(
        &self,
        request: PaxosRpc,
        membership: &MembershipQuorum,
    ) -> mpsc::Receiver<(
        VoterId,
        Result<PaxosRpc, super::transport::tcp::TcpRpcError>,
    )> {
        let peers: Vec<_> = self
            .peers
            .iter()
            .filter(|peer| membership.contains_voter(&peer.voter))
            .cloned()
            .collect();
        let (sender, receiver) = mpsc::channel();
        for peer in peers {
            let sender = sender.clone();
            let client = self.client;
            let request = request.clone();
            thread::spawn(move || {
                let result = client.request_to(&peer.voter, peer.address, request);
                let _ = sender.send((peer.voter, result));
            });
        }
        drop(sender);
        receiver
    }
}
