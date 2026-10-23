mod common;

use common::{stable_quorum, voter, TempDb};
use std::{net::SocketAddr, thread, time::Duration};
use xen_rust_backend::cluster::{
    paxos::{AcceptRequest, Ballot, ChosenValue, WeightedAcceptor},
    paxos_log::{PaxosLogCoordinator, PaxosLogError},
    paxos_round::{PaxosPeerEndpoint, PaxosRoundDriver},
    paxos_storage::SqlitePaxosStore,
    peer::DurablePaxosPeer,
    transport::{tcp::TcpRpcServer, PaxosRpc},
    weighted_quorum::VoterId,
};

fn endpoint(voter: VoterId, address: SocketAddr) -> PaxosPeerEndpoint {
    PaxosPeerEndpoint { voter, address }
}

#[test]
fn coordinator_learns_conflict_then_retries_requested_value_in_next_slot() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut servers = Vec::new();
    let mut endpoints = Vec::new();

    for id in ["b", "c"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(voter(id), server.local_addr().unwrap()));
        servers.push((id.to_string(), server));
    }

    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut acceptor = WeightedAcceptor::new(voter(&id));
                acceptor
                    .handle_accept_durable(
                        AcceptRequest {
                            slot: 1,
                            ballot: Ballot::new(0, voter("a")),
                            value: b"other operation".to_vec(),
                        },
                        &membership,
                        &mut store,
                    )
                    .unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                for _ in 0..6 {
                    server
                        .serve_durable_one(&mut peer, &membership, &mut store)
                        .unwrap();
                }
                (
                    store.commit_index().unwrap(),
                    store.committed_values_after(0, 8).unwrap(),
                )
            })
        })
        .collect();

    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("a"));
    let driver = PaxosRoundDriver::new(Duration::from_secs(2), endpoints).unwrap();
    let mut coordinator = PaxosLogCoordinator::new(voter("a"), driver, 3).unwrap();

    let chosen = coordinator
        .submit(b"our operation".to_vec(), membership, &mut peer, &mut store)
        .unwrap();

    assert_eq!(chosen.slot, 2);
    assert_eq!(chosen.value, b"our operation");
    assert_eq!(store.commit_index().unwrap(), 2);
    assert_eq!(
        store.committed_values_after(0, 8).unwrap(),
        vec![
            ChosenValue {
                slot: 1,
                ballot: Ballot::new(1, voter("a")),
                value: b"other operation".to_vec(),
                acceptors: [voter("b"), voter("c")].into_iter().collect(),
            },
            chosen,
        ]
    );

    for server in server_threads {
        let (commit_index, values) = server.join().unwrap();
        assert_eq!(commit_index, 2);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].value, b"other operation");
        assert_eq!(values[1].value, b"our operation");
    }
}

#[test]
fn coordinator_uses_next_slot_for_later_client_operations() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["b", "c"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(voter(id), server.local_addr().unwrap()));
        servers.push((id.to_string(), server));
    }
    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                for _ in 0..6 {
                    server
                        .serve_durable_one(&mut peer, &membership, &mut store)
                        .unwrap();
                }
                store.committed_values_after(0, 8).unwrap()
            })
        })
        .collect();

    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("a"));
    let mut coordinator = PaxosLogCoordinator::new(
        voter("a"),
        PaxosRoundDriver::new(Duration::from_secs(2), endpoints).unwrap(),
        2,
    )
    .unwrap();
    let first = coordinator
        .submit(b"first".to_vec(), membership.clone(), &mut peer, &mut store)
        .unwrap();
    let second = coordinator
        .submit(b"second".to_vec(), membership, &mut peer, &mut store)
        .unwrap();

    assert_eq!(first.slot, 1);
    assert_eq!(second.slot, 2);
    assert_eq!(store.commit_index().unwrap(), 2);
    for server in server_threads {
        let values = server.join().unwrap();
        assert_eq!(
            values
                .iter()
                .map(|value| value.value.as_slice())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
    }
}

#[test]
fn coordinator_rejects_zero_attempt_limit_and_local_identity_mismatch() {
    let driver = PaxosRoundDriver::new(Duration::from_millis(10), Vec::new()).unwrap();
    assert!(matches!(
        PaxosLogCoordinator::new(voter("a"), driver, 0),
        Err(PaxosLogError::InvalidAttemptLimit)
    ));

    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("b"));
    let mut coordinator = PaxosLogCoordinator::new(
        voter("a"),
        PaxosRoundDriver::new(Duration::from_millis(10), Vec::new()).unwrap(),
        1,
    )
    .unwrap();
    assert!(matches!(
        coordinator.submit(
            b"value".to_vec(),
            stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]),
            &mut peer,
            &mut store,
        ),
        Err(PaxosLogError::ProposerIdentityMismatch { coordinator, peer })
            if coordinator == voter("a") && peer == voter("b")
    ));
    assert_eq!(store.commit_index().unwrap(), 0);
}

#[test]
fn coordinator_retries_preempted_slot_above_observed_ballot() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let server_b =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let server_c =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let endpoints = vec![
        endpoint(voter("b"), server_b.local_addr().unwrap()),
        endpoint(voter("c"), server_c.local_addr().unwrap()),
    ];

    let membership_b = membership.clone();
    let b_thread = thread::spawn(move || {
        let db = TempDb::new();
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("b"));
        server_b
            .serve_one(|request| match request {
                PaxosRpc::Prepare(request) => Ok(PaxosRpc::Promise(
                    xen_rust_backend::cluster::paxos::PromiseResponse::Rejected {
                        slot: request.slot,
                        ballot: request.ballot,
                        voter: voter("b"),
                        promised: Ballot::new(50, voter("b")),
                    },
                )),
                _ => Err("first request must be prepare".to_string()),
            })
            .unwrap();
        for _ in 0..3 {
            server_b
                .serve_durable_one(&mut peer, &membership_b, &mut store)
                .unwrap();
        }
    });
    let membership_c = membership.clone();
    let c_thread = thread::spawn(move || {
        let db = TempDb::new();
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut peer = DurablePaxosPeer::new(voter("c"));
        for _ in 0..4 {
            server_c
                .serve_durable_one(&mut peer, &membership_c, &mut store)
                .unwrap();
        }
    });

    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut peer = DurablePaxosPeer::new(voter("a"));
    let mut coordinator = PaxosLogCoordinator::new(
        voter("a"),
        PaxosRoundDriver::new(Duration::from_secs(2), endpoints).unwrap(),
        2,
    )
    .unwrap();
    let chosen = coordinator
        .submit(
            b"after preemption".to_vec(),
            membership,
            &mut peer,
            &mut store,
        )
        .unwrap();

    assert_eq!(chosen.slot, 1);
    assert_eq!(chosen.ballot, Ballot::new(51, voter("a")));
    assert_eq!(store.commit_index().unwrap(), 1);
    b_thread.join().unwrap();
    c_thread.join().unwrap();
}
