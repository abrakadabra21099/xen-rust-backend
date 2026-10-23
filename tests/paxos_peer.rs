mod common;

use common::{stable_quorum, voter, TempDb};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use xen_rust_backend::cluster::{
    paxos::{
        AcceptRequest, AcceptedResponse, Ballot, ChosenValue, PaxosError, PrepareRequest,
        PromiseResponse,
    },
    paxos_storage::{PaxosStorageError, SqlitePaxosStore},
    peer::{DurablePaxosPeer, DurablePeerError},
    transport::{
        tcp::{TcpRpcClient, TcpRpcServer},
        PaxosRpc,
    },
};

fn chosen(slot: u64, value: &[u8]) -> ChosenValue {
    ChosenValue {
        slot,
        ballot: Ballot::new(1, voter("a")),
        value: value.to_vec(),
        acceptors: [voter("a"), voter("b")].into_iter().collect(),
    }
}

#[test]
fn long_lived_durable_server_answers_multiple_requests_then_stops_cleanly() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let membership_for_server = membership.clone();
    let server_thread = thread::spawn(move || {
        let db = TempDb::new();
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        let served = server
            .serve_durable_until(
                &mut peer,
                &membership_for_server,
                &mut store,
                &server_shutdown,
            )
            .unwrap();
        (served, store.commit_index().unwrap())
    });

    let client = TcpRpcClient::new(Duration::from_secs(2));
    for slot in 1..=3 {
        let ballot = Ballot::new(slot, voter("a"));
        let response = client
            .request(
                address,
                PaxosRpc::Prepare(PrepareRequest {
                    slot,
                    ballot: ballot.clone(),
                }),
            )
            .unwrap();
        assert_eq!(
            response,
            PaxosRpc::Promise(PromiseResponse::Promised {
                slot,
                ballot,
                voter: voter("b"),
                accepted: None,
            })
        );
    }
    shutdown.store(true, Ordering::Release);
    assert_eq!(server_thread.join().unwrap(), (3, 0));
}

#[test]
fn long_lived_durable_server_can_stop_while_idle() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let server_thread = thread::spawn(move || {
        let db = TempDb::new();
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        server
            .serve_durable_until(&mut peer, &membership, &mut store, &server_shutdown)
            .unwrap()
    });

    shutdown.store(true, Ordering::Release);
    assert_eq!(server_thread.join().unwrap(), 0);
}

#[test]
fn prepare_reply_follows_durable_promise_and_rejects_stale_ballots_after_restore() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let promised = Ballot::new(5, voter("a"));
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        let response = peer
            .handle_rpc(
                PaxosRpc::Prepare(PrepareRequest {
                    slot: 1,
                    ballot: promised.clone(),
                }),
                &membership,
                &mut store,
            )
            .unwrap();
        assert_eq!(
            response,
            PaxosRpc::Promise(PromiseResponse::Promised {
                slot: 1,
                ballot: promised.clone(),
                voter: voter("b"),
                accepted: None,
            })
        );
    }

    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::restore(voter("b"), &store).unwrap();
    let stale = peer
        .handle_rpc(
            PaxosRpc::Prepare(PrepareRequest {
                slot: 1,
                ballot: Ballot::new(4, voter("c")),
            }),
            &membership,
            &mut store,
        )
        .unwrap();
    assert_eq!(
        stale,
        PaxosRpc::Promise(PromiseResponse::Rejected {
            slot: 1,
            ballot: Ballot::new(4, voter("c")),
            voter: voter("b"),
            promised,
        })
    );
}

#[test]
fn accept_reply_follows_durable_acceptance_and_survives_restart() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let ballot = Ballot::new(3, voter("a"));
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        let response = peer
            .handle_rpc(
                PaxosRpc::Accept(AcceptRequest {
                    slot: 1,
                    ballot: ballot.clone(),
                    value: b"durable value".to_vec(),
                }),
                &membership,
                &mut store,
            )
            .unwrap();
        assert_eq!(
            response,
            PaxosRpc::Accepted(AcceptedResponse::Accepted {
                slot: 1,
                ballot: ballot.clone(),
                voter: voter("b"),
            })
        );
    }

    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::restore(voter("b"), &store).unwrap();
    let response = peer
        .handle_rpc(
            PaxosRpc::Prepare(PrepareRequest {
                slot: 1,
                ballot: Ballot::new(4, voter("c")),
            }),
            &membership,
            &mut store,
        )
        .unwrap();
    assert!(matches!(
        response,
        PaxosRpc::Promise(PromiseResponse::Promised {
            accepted: Some(accepted), ..
        }) if accepted.value == b"durable value"
    ));
}

#[test]
fn learn_ack_is_returned_only_after_chosen_value_is_durable() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("b"));

    let response = peer
        .handle_rpc(
            PaxosRpc::Learn(chosen(1, b"committed")),
            &membership,
            &mut store,
        )
        .unwrap();

    assert_eq!(
        response,
        PaxosRpc::Learned {
            slot: 1,
            voter: voter("b"),
        }
    );
    assert_eq!(store.commit_index().unwrap(), 1);
    let recovered = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(
        recovered.chosen_value(1).unwrap().unwrap().value,
        b"committed"
    );
}

#[test]
fn worker_and_response_messages_are_rejected_without_persisting_acceptor_state() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut worker = DurablePaxosPeer::new(voter("worker"));
    let request = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: Ballot::new(1, voter("a")),
    });
    assert!(matches!(
        worker.handle_rpc(request, &membership, &mut store),
        Err(DurablePeerError::Storage(PaxosStorageError::Protocol(
            PaxosError::NotVoter(id)
        ))) if id == voter("worker")
    ));

    let mut peer = DurablePaxosPeer::new(voter("b"));
    assert!(matches!(
        peer.handle_rpc(
            PaxosRpc::Promise(PromiseResponse::Promised {
                slot: 1,
                ballot: Ballot::new(1, voter("a")),
                voter: voter("a"),
                accepted: None,
            }),
            &membership,
            &mut store,
        ),
        Err(DurablePeerError::UnexpectedMessageDirection)
    ));
    assert_eq!(store.commit_index().unwrap(), 0);
}

#[test]
fn learn_with_insufficient_weighted_certificate_is_not_acknowledged_or_saved() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 2), ("b", 1), ("c", 2)]);
    let mut insufficient = chosen(1, b"uncommitted");
    insufficient.acceptors = [voter("b")].into_iter().collect();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("b"));

    assert!(matches!(
        peer.handle_rpc(PaxosRpc::Learn(insufficient), &membership, &mut store),
        Err(DurablePeerError::Storage(PaxosStorageError::Protocol(
            PaxosError::InsufficientQuorum
        )))
    ));
    assert_eq!(store.commit_index().unwrap(), 0);
    assert_eq!(store.chosen_value(1).unwrap(), None);
}

#[test]
fn tcp_learn_ack_follows_durable_peer_commit() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let server = TcpRpcServer::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        Duration::from_secs(2),
    )
    .unwrap();
    let address = server.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        server
            .serve_durable_one(&mut peer, &membership, &mut store)
            .unwrap();
        store.commit_index().unwrap()
    });

    let response = TcpRpcClient::new(Duration::from_secs(2))
        .request(address, PaxosRpc::Learn(chosen(1, b"network committed")))
        .unwrap();
    assert_eq!(
        response,
        PaxosRpc::Learned {
            slot: 1,
            voter: voter("b"),
        }
    );
    assert_eq!(server_thread.join().unwrap(), 1);
}
