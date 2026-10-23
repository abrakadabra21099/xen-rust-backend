mod common;

use common::{stable_quorum, voter, TempDb};
use std::{net::SocketAddr, thread, time::Duration};
use xen_rust_backend::cluster::{
    membership_state::MembershipQuorum,
    paxos::{Ballot, PromiseResponse},
    paxos_round::{PaxosPeerEndpoint, PaxosRoundDriver, PaxosRoundError, PaxosRoundPhase},
    paxos_storage::SqlitePaxosStore,
    peer::DurablePaxosPeer,
    transport::{tcp::TcpRpcServer, PaxosRpc},
    weighted_quorum::MembershipChange,
};

fn endpoint(voter_name: &str, address: SocketAddr) -> PaxosPeerEndpoint {
    PaxosPeerEndpoint {
        voter: voter(voter_name),
        address,
    }
}

#[test]
fn network_round_reaches_weighted_quorum_and_durably_learns_on_nodes() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["a", "b", "c"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(id, server.local_addr().unwrap()));
        servers.push((id, server));
    }

    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let id = id.to_string();
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                for _ in 0..3 {
                    server
                        .serve_durable_one(&mut peer, &membership, &mut store)
                        .unwrap();
                }
                store.commit_index().unwrap()
            })
        })
        .collect();

    let driver = PaxosRoundDriver::new(Duration::from_secs(2), endpoints).unwrap();
    let outcome = driver
        .run_round(
            1,
            Ballot::new(1, voter("a")),
            b"network value".to_vec(),
            membership.clone(),
        )
        .unwrap();

    assert_eq!(outcome.chosen.value, b"network value");
    assert_eq!(outcome.chosen.slot, 1);
    assert!(membership
        .has_quorum(outcome.chosen.acceptors.iter().cloned())
        .unwrap());
    assert!(outcome.learned_quorum);
    assert!(membership
        .has_quorum(outcome.learned_voters.iter().cloned())
        .unwrap());
    for server in server_threads {
        assert_eq!(server.join().unwrap(), 1);
    }
}

#[test]
fn network_round_uses_joint_quorums_from_both_membership_epochs() {
    let old = common::membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let joint = old
        .begin_change(MembershipChange::AddVoter {
            id: voter("d"),
            weight: 1,
        })
        .unwrap();
    let membership = MembershipQuorum::Joint(joint);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["b", "c", "d"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(id, server.local_addr().unwrap()));
        servers.push((id, server));
    }
    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let id = id.to_string();
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                for _ in 0..3 {
                    server
                        .serve_durable_one(&mut peer, &membership, &mut store)
                        .unwrap();
                }
                store.commit_index().unwrap()
            })
        })
        .collect();

    let outcome = PaxosRoundDriver::new(Duration::from_secs(2), endpoints)
        .unwrap()
        .run_round(
            1,
            Ballot::new(1, voter("b")),
            b"joint value".to_vec(),
            membership.clone(),
        )
        .unwrap();

    assert_eq!(outcome.chosen.value, b"joint value");
    assert!(membership
        .has_quorum(outcome.chosen.acceptors.iter().cloned())
        .unwrap());
    assert!(outcome.learned_quorum);
    assert!(membership
        .has_quorum(outcome.learned_voters.iter().cloned())
        .unwrap());
    for server in server_threads {
        assert_eq!(server.join().unwrap(), 1);
    }
}

#[test]
fn network_round_rejects_a_prepare_set_that_quorums_only_the_old_membership() {
    let old = common::membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let joint = old
        .begin_change(MembershipChange::AddVoter {
            id: voter("d"),
            weight: 1,
        })
        .unwrap();
    let membership = MembershipQuorum::Joint(joint);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["b", "c"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(id, server.local_addr().unwrap()));
        servers.push((id, server));
    }
    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let id = id.to_string();
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                server
                    .serve_durable_one(&mut peer, &membership, &mut store)
                    .unwrap();
            })
        })
        .collect();

    let result = PaxosRoundDriver::new(Duration::from_secs(2), endpoints)
        .unwrap()
        .run_round(
            1,
            Ballot::new(1, voter("b")),
            b"must not be chosen".to_vec(),
            membership,
        );
    assert!(matches!(
        result,
        Err(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Prepare))
    ));
    for server in server_threads {
        server.join().unwrap();
    }
}

#[test]
fn round_requires_prepare_quorum_and_rejects_invalid_slots() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let driver = PaxosRoundDriver::new(Duration::from_millis(20), Vec::new()).unwrap();
    assert!(matches!(
        driver.run_round(
            1,
            Ballot::new(1, voter("a")),
            b"value".to_vec(),
            membership.clone()
        ),
        Err(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Prepare))
    ));
    assert!(matches!(
        driver.run_round(0, Ballot::new(1, voter("a")), b"value".to_vec(), membership),
        Err(PaxosRoundError::InvalidProtocol(
            xen_rust_backend::cluster::paxos::PaxosError::InvalidSlot
        ))
    ));
}

#[test]
fn duplicate_voter_endpoints_are_rejected_before_a_round_starts() {
    let address = "127.0.0.1:1".parse().unwrap();
    assert!(matches!(
        PaxosRoundDriver::new(
            Duration::from_secs(1),
            vec![endpoint("a", address), endpoint("a", address)]
        ),
        Err(PaxosRoundError::DuplicatePeer(id)) if id == voter("a")
    ));
}

#[test]
fn higher_promise_preempts_the_round_without_sending_accept() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let driver = PaxosRoundDriver::new(
        Duration::from_secs(2),
        vec![endpoint("b", server.local_addr().unwrap())],
    )
    .unwrap();
    let server_thread = thread::spawn(move || {
        server
            .serve_one(|request| match request {
                PaxosRpc::Prepare(request) => Ok(PaxosRpc::Promise(PromiseResponse::Rejected {
                    slot: request.slot,
                    ballot: request.ballot,
                    voter: voter("b"),
                    promised: Ballot::new(2, voter("b")),
                })),
                _ => Err("only prepare is expected".to_string()),
            })
            .unwrap();
    });

    let result = driver.run_round(
        1,
        Ballot::new(1, voter("a")),
        b"value".to_vec(),
        stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]),
    );
    assert!(matches!(
        result,
        Err(PaxosRoundError::Preempted(ballot)) if ballot == Ballot::new(2, voter("b"))
    ));
    server_thread.join().unwrap();
}

#[test]
fn chosen_value_is_reported_even_if_learn_quorum_is_unavailable() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["a", "b"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(id, server.local_addr().unwrap()));
        servers.push((id, server));
    }

    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let id = id.to_string();
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                for request_index in 0..3 {
                    if id == "b" && request_index == 2 {
                        server
                            .serve_one(|request| match request {
                                PaxosRpc::Learn(_) => Err("learning disabled".to_string()),
                                _ => Err("unexpected request".to_string()),
                            })
                            .unwrap_err();
                    } else {
                        server
                            .serve_durable_one(&mut peer, &membership, &mut store)
                            .unwrap();
                    }
                }
                (id, store.commit_index().unwrap())
            })
        })
        .collect();

    let outcome = PaxosRoundDriver::new(Duration::from_secs(2), endpoints)
        .unwrap()
        .run_round(
            1,
            Ballot::new(1, voter("a")),
            b"chosen".to_vec(),
            membership,
        )
        .unwrap();

    assert_eq!(outcome.chosen.value, b"chosen");
    assert!(!outcome.learned_quorum);
    assert_eq!(outcome.learned_voters, [voter("a")].into_iter().collect());
    for server in server_threads {
        let (id, commit_index) = server.join().unwrap();
        assert_eq!(commit_index, if id == "a" { 1 } else { 0 });
    }
}

#[test]
fn absent_acceptors_are_not_counted_toward_the_prepare_quorum() {
    let server =
        TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
    let address = server.local_addr().unwrap();
    let membership: MembershipQuorum = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let driver =
        PaxosRoundDriver::new(Duration::from_secs(2), vec![endpoint("a", address)]).unwrap();
    let server_thread = thread::spawn(move || {
        let mut peer = DurablePaxosPeer::new(voter("a"));
        let db = TempDb::new();
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        server
            .serve_durable_one(&mut peer, &membership, &mut store)
            .unwrap();
    });

    assert!(matches!(
        driver.run_round(
            1,
            Ballot::new(1, voter("a")),
            b"value".to_vec(),
            stable_quorum(&[("a", 1), ("b", 1), ("c", 1)])
        ),
        Err(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Prepare))
    ));
    server_thread.join().unwrap();
}

#[test]
fn prepare_quorum_without_accept_quorum_does_not_choose_a_value() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut endpoints = Vec::new();
    let mut servers = Vec::new();
    for id in ["a", "b"] {
        let server =
            TcpRpcServer::bind("127.0.0.1:0".parse().unwrap(), Duration::from_secs(2)).unwrap();
        endpoints.push(endpoint(id, server.local_addr().unwrap()));
        servers.push((id, server));
    }
    let server_threads: Vec<_> = servers
        .into_iter()
        .map(|(id, server)| {
            let id = id.to_string();
            let membership = membership.clone();
            thread::spawn(move || {
                let db = TempDb::new();
                let mut store = SqlitePaxosStore::open(db.path()).unwrap();
                let mut peer = DurablePaxosPeer::new(voter(&id));
                if id == "a" {
                    for _ in 0..2 {
                        server
                            .serve_durable_one(&mut peer, &membership, &mut store)
                            .unwrap();
                    }
                } else {
                    server
                        .serve_one(|request| match request {
                            PaxosRpc::Prepare(request) => {
                                Ok(PaxosRpc::Promise(PromiseResponse::Promised {
                                    slot: request.slot,
                                    ballot: request.ballot,
                                    voter: voter("b"),
                                    accepted: None,
                                }))
                            }
                            _ => Err("unexpected first request".to_string()),
                        })
                        .unwrap();
                    server
                        .serve_one(|_| Err("accept phase unavailable".to_string()))
                        .unwrap_err();
                }
            })
        })
        .collect();

    let result = PaxosRoundDriver::new(Duration::from_secs(2), endpoints)
        .unwrap()
        .run_round(
            1,
            Ballot::new(1, voter("a")),
            b"uncommitted".to_vec(),
            membership,
        );
    assert!(matches!(
        result,
        Err(PaxosRoundError::QuorumUnavailable(PaxosRoundPhase::Accept))
    ));
    for server in server_threads {
        server.join().unwrap();
    }
}
