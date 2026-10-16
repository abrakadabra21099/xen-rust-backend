mod common;

use common::voter;
use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};
use xen_rust_backend::cluster::paxos::{
    AcceptRequest, AcceptedResponse, Ballot, ChosenValue, PrepareRequest, PromiseResponse,
};
use xen_rust_backend::cluster::transport::{
    tcp::{TcpRpcClient, TcpRpcError, TcpRpcServer},
    PaxosRpc,
};

fn address(listener: &TcpListener) -> SocketAddr {
    listener.local_addr().unwrap()
}

fn send_raw_message(stream: &mut TcpStream, message: &PaxosRpc) {
    let payload = message.encode().unwrap();
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

#[test]
fn tcp_client_and_server_exchange_prepare_accept_and_learn_rpcs() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..3 {
            server
                .serve_one(|request| {
                    let response = match request {
                        PaxosRpc::Prepare(request) => {
                            PaxosRpc::Promise(PromiseResponse::Promised {
                                slot: request.slot,
                                ballot: request.ballot,
                                voter: voter("node-b"),
                                accepted: None,
                            })
                        }
                        PaxosRpc::Accept(request) => {
                            PaxosRpc::Accepted(AcceptedResponse::Accepted {
                                slot: request.slot,
                                ballot: request.ballot,
                                voter: voter("node-b"),
                            })
                        }
                        PaxosRpc::Learn(chosen) => PaxosRpc::Learned {
                            slot: chosen.slot,
                            voter: voter("node-b"),
                        },
                        _ => return Err("expected a Paxos request".to_string()),
                    };
                    Ok(response)
                })
                .unwrap();
        }
    });

    let client = TcpRpcClient::new(Duration::from_secs(2));
    let ballot = Ballot::new(3, voter("node-a"));
    let prepare = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot.clone(),
    });
    assert_eq!(
        client.request(peer_address, prepare).unwrap(),
        PaxosRpc::Promise(PromiseResponse::Promised {
            slot: 1,
            ballot: ballot.clone(),
            voter: voter("node-b"),
            accepted: None,
        })
    );

    let accept = PaxosRpc::Accept(AcceptRequest {
        slot: 2,
        ballot: ballot.clone(),
        value: b"value".to_vec(),
    });
    assert_eq!(
        client.request(peer_address, accept).unwrap(),
        PaxosRpc::Accepted(AcceptedResponse::Accepted {
            slot: 2,
            ballot: ballot.clone(),
            voter: voter("node-b"),
        })
    );

    let learn = PaxosRpc::Learn(ChosenValue {
        slot: 3,
        ballot,
        value: b"chosen".to_vec(),
        acceptors: [voter("node-a"), voter("node-b")].into_iter().collect(),
    });
    assert_eq!(
        client.request(peer_address, learn).unwrap(),
        PaxosRpc::Learned {
            slot: 3,
            voter: voter("node-b"),
        }
    );
    server_thread.join().unwrap();
}

#[test]
fn client_rejects_response_messages_as_requests_without_connecting() {
    let client = TcpRpcClient::new(Duration::from_secs(1));
    let not_a_request = PaxosRpc::Promise(PromiseResponse::Promised {
        slot: 1,
        ballot: Ballot::new(1, voter("node-a")),
        voter: voter("node-b"),
        accepted: None,
    });
    assert!(matches!(
        client.request("127.0.0.1:1".parse().unwrap(), not_a_request),
        Err(TcpRpcError::UnexpectedRequest)
    ));
}

#[test]
fn server_rejects_non_request_messages_and_does_not_call_handler() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        server.serve_one(|_| panic!("handler must not receive a response message"))
    });

    let mut stream = TcpStream::connect(peer_address).unwrap();
    send_raw_message(
        &mut stream,
        &PaxosRpc::Learned {
            slot: 1,
            voter: voter("node-b"),
        },
    );
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::UnexpectedRequest)
    ));
}

#[test]
fn server_rejects_mismatched_handler_responses_before_sending_them() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        server.serve_one(|_| {
            Ok(PaxosRpc::Promise(PromiseResponse::Promised {
                slot: 2,
                ballot: Ballot::new(1, voter("node-a")),
                voter: voter("node-b"),
                accepted: None,
            }))
        })
    });

    let request = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: Ballot::new(1, voter("node-a")),
    });
    let client = TcpRpcClient::new(Duration::from_secs(2));
    assert!(matches!(
        client.request(peer_address, request),
        Err(TcpRpcError::Io(_))
    ));
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::MismatchedResponse)
    ));
}

#[test]
fn server_rejects_a_handler_that_returns_a_request() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || server.serve_one(|request| Ok(request)));

    let mut stream = TcpStream::connect(peer_address).unwrap();
    send_raw_message(
        &mut stream,
        &PaxosRpc::Prepare(PrepareRequest {
            slot: 1,
            ballot: Ballot::new(1, voter("node-a")),
        }),
    );
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::UnexpectedResponse)
    ));
}

#[test]
fn server_reports_handler_failures_without_sending_a_response() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        server.serve_one(|_| Err("membership state unavailable".to_string()))
    });

    let mut stream = TcpStream::connect(peer_address).unwrap();
    send_raw_message(
        &mut stream,
        &PaxosRpc::Prepare(PrepareRequest {
            slot: 1,
            ballot: Ballot::new(1, voter("node-a")),
        }),
    );
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::Handler(message)) if message == "membership state unavailable"
    ));
}

#[test]
fn client_rejects_a_peer_that_sends_a_request_instead_of_a_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_address = address(&listener);
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        send_raw_message(
            &mut stream,
            &PaxosRpc::Prepare(PrepareRequest {
                slot: 1,
                ballot: Ballot::new(1, voter("node-a")),
            }),
        );
    });

    let request = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: Ballot::new(1, voter("node-a")),
    });
    assert!(matches!(
        TcpRpcClient::new(Duration::from_secs(2)).request(peer_address, request),
        Err(TcpRpcError::UnexpectedResponse)
    ));
    server_thread.join().unwrap();
}

#[test]
fn server_reports_truncated_frames_as_io_errors() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || server.serve_one(|_| unreachable!()));

    let mut stream = TcpStream::connect(peer_address).unwrap();
    stream.write_all(&8_u32.to_be_bytes()).unwrap();
    stream.write_all(b"XP").unwrap();
    drop(stream);
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::Io(_))
    ));
}

#[test]
fn server_rejects_oversized_frame_before_allocating_its_payload() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let peer_address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        server.serve_one(|_| panic!("oversized frame must not reach the handler"))
    });

    let mut stream = TcpStream::connect(peer_address).unwrap();
    stream
        .write_all(&((16 * 1024 * 1024 + 1) as u32).to_be_bytes())
        .unwrap();
    assert!(matches!(
        server_thread.join().unwrap(),
        Err(TcpRpcError::FrameTooLarge(16_777_217))
    ));
}

#[test]
fn client_rejects_a_peer_response_for_another_slot() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_address = address(&listener);
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut prefix = [0; 4];
        std::io::Read::read_exact(&mut stream, &mut prefix).unwrap();
        let mut payload = vec![0; u32::from_be_bytes(prefix) as usize];
        std::io::Read::read_exact(&mut stream, &mut payload).unwrap();
        send_raw_message(
            &mut stream,
            &PaxosRpc::Promise(PromiseResponse::Promised {
                slot: 9,
                ballot: Ballot::new(1, voter("node-a")),
                voter: voter("node-b"),
                accepted: None,
            }),
        );
    });

    let request = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: Ballot::new(1, voter("node-a")),
    });
    assert!(matches!(
        TcpRpcClient::new(Duration::from_secs(2)).request(peer_address, request),
        Err(TcpRpcError::MismatchedResponse)
    ));
    server_thread.join().unwrap();
}
