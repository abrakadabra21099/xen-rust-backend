//! Blocking TCP request/reply transport for Paxos peer RPCs.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    time::Duration,
};

use super::{PaxosRpc, RpcCodecError, MAX_MESSAGE_BYTES};
use crate::cluster::{
    membership_state::MembershipQuorum, paxos_storage::SqlitePaxosStore, peer::DurablePaxosPeer,
    weighted_quorum::VoterId,
};

/// Failures while sending or serving one framed TCP RPC.
#[derive(Debug)]
pub enum TcpRpcError {
    Io(std::io::Error),
    Codec(RpcCodecError),
    FrameTooLarge(usize),
    UnexpectedRequest,
    UnexpectedResponse,
    MismatchedResponse,
    PeerIdentityMismatch { expected: VoterId, actual: VoterId },
    Handler(String),
}

impl std::fmt::Display for TcpRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "TCP RPC I/O error: {error}"),
            Self::Codec(error) => write!(f, "TCP RPC codec error: {error}"),
            Self::FrameTooLarge(size) => {
                write!(
                    f,
                    "TCP RPC frame size {size} exceeds the configured maximum"
                )
            }
            Self::UnexpectedRequest => write!(f, "TCP RPC message is not a request"),
            Self::UnexpectedResponse => write!(f, "TCP RPC handler returned a request"),
            Self::MismatchedResponse => write!(f, "TCP RPC response does not match its request"),
            Self::PeerIdentityMismatch { expected, actual } => write!(
                f,
                "TCP RPC endpoint for {} returned a response claiming to be {}",
                expected.as_str(),
                actual.as_str()
            ),
            Self::Handler(message) => write!(f, "TCP RPC handler failed: {message}"),
        }
    }
}

impl std::error::Error for TcpRpcError {}

impl From<std::io::Error> for TcpRpcError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<RpcCodecError> for TcpRpcError {
    fn from(error: RpcCodecError) -> Self {
        Self::Codec(error)
    }
}

/// A blocking TCP client. Each call uses one connection for one RPC exchange.
#[derive(Clone, Copy, Debug)]
pub struct TcpRpcClient {
    timeout: Duration,
}

impl TcpRpcClient {
    /// Creates a client with a connection, read, and write timeout.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Sends a request and validates that the peer returned the matching response.
    pub fn request(&self, address: SocketAddr, request: PaxosRpc) -> Result<PaxosRpc, TcpRpcError> {
        self.request_inner(address, request, None)
    }

    /// Sends a request and checks the voter ID claimed in the response.
    ///
    /// This detects endpoint misconfiguration but is not cryptographic
    /// authentication; an untrusted peer can still claim another identity.
    pub fn request_to(
        &self,
        expected_peer: &VoterId,
        address: SocketAddr,
        request: PaxosRpc,
    ) -> Result<PaxosRpc, TcpRpcError> {
        self.request_inner(address, request, Some(expected_peer))
    }

    fn request_inner(
        &self,
        address: SocketAddr,
        request: PaxosRpc,
        expected_peer: Option<&VoterId>,
    ) -> Result<PaxosRpc, TcpRpcError> {
        if !request.is_request() {
            return Err(TcpRpcError::UnexpectedRequest);
        }
        let mut stream = TcpStream::connect_timeout(&address, self.timeout)?;
        configure_stream(&stream, self.timeout)?;
        write_frame(&mut stream, &request)?;
        let response = read_frame(&mut stream)?;
        if !response.is_response() {
            return Err(TcpRpcError::UnexpectedResponse);
        }
        if !request.response_matches(&response) {
            return Err(TcpRpcError::MismatchedResponse);
        }
        if let Some(expected) = expected_peer {
            if let Some(actual) = response.response_voter() {
                if expected != actual {
                    return Err(TcpRpcError::PeerIdentityMismatch {
                        expected: expected.clone(),
                        actual: actual.clone(),
                    });
                }
            }
        }
        Ok(response)
    }
}

/// A TCP endpoint that accepts and handles one RPC at a time.
///
/// The caller can run `serve_one` in its own loop or on a worker thread. Each
/// accepted connection carries exactly one request and one response.
pub struct TcpRpcServer {
    listener: TcpListener,
    timeout: Duration,
}

impl TcpRpcServer {
    /// Binds a local address for peer connections.
    pub fn bind(address: SocketAddr, timeout: Duration) -> Result<Self, TcpRpcError> {
        let listener = TcpListener::bind(address)?;
        Ok(Self { listener, timeout })
    }

    /// Returns the actual bound address, useful when binding port zero.
    pub fn local_addr(&self) -> Result<SocketAddr, TcpRpcError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accepts one connection, calls the handler, and sends its matching response.
    pub fn serve_one<F>(&self, handler: F) -> Result<(), TcpRpcError>
    where
        F: FnOnce(PaxosRpc) -> Result<PaxosRpc, String>,
    {
        let (mut stream, _) = self.listener.accept()?;
        configure_stream(&stream, self.timeout)?;
        let request = read_frame(&mut stream)?;
        if !request.is_request() {
            return Err(TcpRpcError::UnexpectedRequest);
        }
        let response = handler(request.clone()).map_err(TcpRpcError::Handler)?;
        if !response.is_response() {
            return Err(TcpRpcError::UnexpectedResponse);
        }
        if !request.response_matches(&response) {
            return Err(TcpRpcError::MismatchedResponse);
        }
        write_frame(&mut stream, &response)
    }

    /// Serves one request through a durable local acceptor and replies after its
    /// storage transaction has committed.
    pub fn serve_durable_one(
        &self,
        peer: &mut DurablePaxosPeer,
        membership: &MembershipQuorum,
        store: &mut SqlitePaxosStore,
    ) -> Result<(), TcpRpcError> {
        self.serve_one(|request| {
            peer.handle_rpc(request, membership, store)
                .map_err(|error| error.to_string())
        })
    }
}

fn configure_stream(stream: &TcpStream, timeout: Duration) -> Result<(), TcpRpcError> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.set_nodelay(true)?;
    Ok(())
}

fn write_frame(stream: &mut TcpStream, message: &PaxosRpc) -> Result<(), TcpRpcError> {
    let payload = message.encode()?;
    let length =
        u32::try_from(payload.len()).map_err(|_| TcpRpcError::FrameTooLarge(payload.len()))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> Result<PaxosRpc, TcpRpcError> {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err(TcpRpcError::FrameTooLarge(length));
    }
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload)?;
    Ok(PaxosRpc::decode(&payload)?)
}
