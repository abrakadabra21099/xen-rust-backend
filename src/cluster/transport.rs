//! Versioned, bounded wire format for cleartext Paxos peer messages.
//!
//! The parent module defines the bounded message encoding. The `tcp` submodule
//! provides blocking, one-request-per-connection RPC calls. Peer authentication,
//! encryption, retries, and long-running server lifecycle are not included.

pub mod tcp;

use std::collections::BTreeSet;

use super::paxos::{
    AcceptRequest, AcceptedResponse, AcceptedValue, Ballot, ChosenValue, PrepareRequest,
    PromiseResponse,
};
use super::weighted_quorum::VoterId;

const MAGIC: &[u8; 4] = b"XPAX";
const VERSION: u8 = 1;
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_COLLECTION_ITEMS: usize = 4096;
const MAX_VOTER_ID_BYTES: usize = 1024;

/// One peer-to-peer message in the Paxos protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaxosRpc {
    Prepare(PrepareRequest),
    Promise(PromiseResponse),
    Accept(AcceptRequest),
    Accepted(AcceptedResponse),
    Learn(ChosenValue),
    Learned { slot: u64, voter: VoterId },
}

/// Errors while encoding or decoding a peer message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcCodecError {
    MessageTooLarge,
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownMessageKind(u8),
    InvalidTag(u8),
    InvalidUtf8,
    InvalidVoterId,
    VoterIdTooLong,
    DuplicateAcceptor(VoterId),
    TooManyItems,
    TrailingBytes,
}

impl std::fmt::Display for RpcCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MessageTooLarge => write!(f, "Paxos RPC exceeds the maximum message size"),
            Self::Truncated => write!(f, "Paxos RPC ended before the message was complete"),
            Self::InvalidMagic => write!(f, "invalid Paxos RPC magic header"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported Paxos RPC version {version}")
            }
            Self::UnknownMessageKind(kind) => write!(f, "unknown Paxos RPC kind {kind}"),
            Self::InvalidTag(tag) => write!(f, "invalid Paxos RPC field tag {tag}"),
            Self::InvalidUtf8 => write!(f, "Paxos RPC voter ID is not valid UTF-8"),
            Self::InvalidVoterId => write!(f, "Paxos RPC contains an invalid voter ID"),
            Self::VoterIdTooLong => write!(f, "Paxos RPC voter ID exceeds its byte limit"),
            Self::DuplicateAcceptor(id) => {
                write!(f, "Paxos RPC repeats acceptor {}", id.as_str())
            }
            Self::TooManyItems => write!(f, "Paxos RPC collection exceeds its item limit"),
            Self::TrailingBytes => write!(f, "Paxos RPC contains trailing bytes"),
        }
    }
}

impl std::error::Error for RpcCodecError {}

impl PaxosRpc {
    /// Encodes one message. Socket framing is intentionally left to the transport.
    pub fn encode(&self) -> Result<Vec<u8>, RpcCodecError> {
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.push(VERSION);
        match self {
            Self::Prepare(request) => {
                output.push(0);
                put_u64(&mut output, request.slot);
                put_ballot(&mut output, &request.ballot)?;
            }
            Self::Promise(response) => {
                output.push(1);
                match response {
                    PromiseResponse::Promised {
                        slot,
                        ballot,
                        voter,
                        accepted,
                    } => {
                        output.push(0);
                        put_u64(&mut output, *slot);
                        put_ballot(&mut output, ballot)?;
                        put_voter(&mut output, voter)?;
                        match accepted {
                            Some(accepted) => {
                                output.push(1);
                                put_ballot(&mut output, &accepted.ballot)?;
                                put_bytes(&mut output, &accepted.value)?;
                            }
                            None => output.push(0),
                        }
                    }
                    PromiseResponse::Rejected {
                        slot,
                        ballot,
                        voter,
                        promised,
                    } => {
                        output.push(1);
                        put_u64(&mut output, *slot);
                        put_ballot(&mut output, ballot)?;
                        put_voter(&mut output, voter)?;
                        put_ballot(&mut output, promised)?;
                    }
                }
            }
            Self::Accept(request) => {
                output.push(2);
                put_u64(&mut output, request.slot);
                put_ballot(&mut output, &request.ballot)?;
                put_bytes(&mut output, &request.value)?;
            }
            Self::Accepted(response) => {
                output.push(3);
                match response {
                    AcceptedResponse::Accepted {
                        slot,
                        ballot,
                        voter,
                    } => {
                        output.push(0);
                        put_u64(&mut output, *slot);
                        put_ballot(&mut output, ballot)?;
                        put_voter(&mut output, voter)?;
                    }
                    AcceptedResponse::Rejected {
                        slot,
                        ballot,
                        voter,
                        promised,
                    } => {
                        output.push(1);
                        put_u64(&mut output, *slot);
                        put_ballot(&mut output, ballot)?;
                        put_voter(&mut output, voter)?;
                        put_ballot(&mut output, promised)?;
                    }
                }
            }
            Self::Learn(chosen) => {
                output.push(4);
                put_u64(&mut output, chosen.slot);
                put_ballot(&mut output, &chosen.ballot)?;
                put_bytes(&mut output, &chosen.value)?;
                put_count(&mut output, chosen.acceptors.len())?;
                for acceptor in &chosen.acceptors {
                    put_voter(&mut output, acceptor)?;
                }
            }
            Self::Learned { slot, voter } => {
                output.push(5);
                put_u64(&mut output, *slot);
                put_voter(&mut output, voter)?;
            }
        }
        if output.len() > MAX_MESSAGE_BYTES {
            return Err(RpcCodecError::MessageTooLarge);
        }
        Ok(output)
    }

    /// Decodes exactly one message and rejects unsupported versions or trailing data.
    pub fn decode(bytes: &[u8]) -> Result<Self, RpcCodecError> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(RpcCodecError::MessageTooLarge);
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(RpcCodecError::InvalidMagic);
        }
        let version = reader.byte()?;
        if version != VERSION {
            return Err(RpcCodecError::UnsupportedVersion(version));
        }
        let message = match reader.byte()? {
            0 => Self::Prepare(PrepareRequest {
                slot: reader.u64()?,
                ballot: reader.ballot()?,
            }),
            1 => Self::Promise(reader.promise()?),
            2 => Self::Accept(AcceptRequest {
                slot: reader.u64()?,
                ballot: reader.ballot()?,
                value: reader.bytes()?,
            }),
            3 => Self::Accepted(reader.accepted_response()?),
            4 => Self::Learn(reader.chosen()?),
            5 => Self::Learned {
                slot: reader.u64()?,
                voter: reader.voter()?,
            },
            other => return Err(RpcCodecError::UnknownMessageKind(other)),
        };
        if !reader.is_empty() {
            return Err(RpcCodecError::TrailingBytes);
        }
        Ok(message)
    }
}

impl PaxosRpc {
    fn is_request(&self) -> bool {
        matches!(self, Self::Prepare(_) | Self::Accept(_) | Self::Learn(_))
    }

    fn is_response(&self) -> bool {
        matches!(
            self,
            Self::Promise(_) | Self::Accepted(_) | Self::Learned { .. }
        )
    }

    fn response_matches(&self, response: &Self) -> bool {
        match (self, response) {
            (
                Self::Prepare(request),
                Self::Promise(PromiseResponse::Promised { slot, ballot, .. }),
            )
            | (
                Self::Prepare(request),
                Self::Promise(PromiseResponse::Rejected { slot, ballot, .. }),
            ) => request.slot == *slot && request.ballot == *ballot,
            (
                Self::Accept(request),
                Self::Accepted(AcceptedResponse::Accepted { slot, ballot, .. }),
            )
            | (
                Self::Accept(request),
                Self::Accepted(AcceptedResponse::Rejected { slot, ballot, .. }),
            ) => request.slot == *slot && request.ballot == *ballot,
            (Self::Learn(chosen), Self::Learned { slot, .. }) => chosen.slot == *slot,
            _ => false,
        }
    }
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_count(output: &mut Vec<u8>, count: usize) -> Result<(), RpcCodecError> {
    if count > MAX_COLLECTION_ITEMS {
        return Err(RpcCodecError::TooManyItems);
    }
    output.extend_from_slice(&(count as u32).to_be_bytes());
    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), RpcCodecError> {
    if value.len() > MAX_MESSAGE_BYTES {
        return Err(RpcCodecError::MessageTooLarge);
    }
    let length = u32::try_from(value.len()).map_err(|_| RpcCodecError::MessageTooLarge)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn put_voter(output: &mut Vec<u8>, voter: &VoterId) -> Result<(), RpcCodecError> {
    let value = voter.as_str().as_bytes();
    if value.len() > MAX_VOTER_ID_BYTES {
        return Err(RpcCodecError::VoterIdTooLong);
    }
    put_bytes(output, value)
}

fn put_ballot(output: &mut Vec<u8>, ballot: &Ballot) -> Result<(), RpcCodecError> {
    put_u64(output, ballot.round());
    put_voter(output, ballot.proposer())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], RpcCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RpcCodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(RpcCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, RpcCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, RpcCodecError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, RpcCodecError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, RpcCodecError> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }

    fn voter(&mut self) -> Result<VoterId, RpcCodecError> {
        let bytes = self.bytes()?;
        if bytes.len() > MAX_VOTER_ID_BYTES {
            return Err(RpcCodecError::VoterIdTooLong);
        }
        let value = std::str::from_utf8(&bytes).map_err(|_| RpcCodecError::InvalidUtf8)?;
        VoterId::new(value).map_err(|_| RpcCodecError::InvalidVoterId)
    }

    fn ballot(&mut self) -> Result<Ballot, RpcCodecError> {
        Ok(Ballot::new(self.u64()?, self.voter()?))
    }

    fn promise(&mut self) -> Result<PromiseResponse, RpcCodecError> {
        let tag = self.byte()?;
        let slot = self.u64()?;
        let ballot = self.ballot()?;
        let voter = self.voter()?;
        match tag {
            0 => {
                let accepted = match self.byte()? {
                    0 => None,
                    1 => Some(AcceptedValue {
                        ballot: self.ballot()?,
                        value: self.bytes()?,
                    }),
                    tag => return Err(RpcCodecError::InvalidTag(tag)),
                };
                Ok(PromiseResponse::Promised {
                    slot,
                    ballot,
                    voter,
                    accepted,
                })
            }
            1 => Ok(PromiseResponse::Rejected {
                slot,
                ballot,
                voter,
                promised: self.ballot()?,
            }),
            other => Err(RpcCodecError::InvalidTag(other)),
        }
    }

    fn accepted_response(&mut self) -> Result<AcceptedResponse, RpcCodecError> {
        let tag = self.byte()?;
        let slot = self.u64()?;
        let ballot = self.ballot()?;
        let voter = self.voter()?;
        match tag {
            0 => Ok(AcceptedResponse::Accepted {
                slot,
                ballot,
                voter,
            }),
            1 => Ok(AcceptedResponse::Rejected {
                slot,
                ballot,
                voter,
                promised: self.ballot()?,
            }),
            other => Err(RpcCodecError::InvalidTag(other)),
        }
    }

    fn chosen(&mut self) -> Result<ChosenValue, RpcCodecError> {
        let slot = self.u64()?;
        let ballot = self.ballot()?;
        let value = self.bytes()?;
        let count = self.u32()? as usize;
        if count > MAX_COLLECTION_ITEMS {
            return Err(RpcCodecError::TooManyItems);
        }
        let mut acceptors = BTreeSet::new();
        for _ in 0..count {
            let voter = self.voter()?;
            if !acceptors.insert(voter.clone()) {
                return Err(RpcCodecError::DuplicateAcceptor(voter));
            }
        }
        Ok(ChosenValue {
            slot,
            ballot,
            value,
            acceptors,
        })
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
