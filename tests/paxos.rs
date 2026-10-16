mod common;

use common::{stable_quorum, voter};
use xen_rust_backend::cluster::membership_state::MembershipQuorum;
use xen_rust_backend::cluster::paxos::{
    AcceptProgress, AcceptRequest, AcceptedResponse, Ballot, ChosenValue, PaxosError,
    PrepareRequest, PromiseProgress, PromiseResponse, WeightedAcceptor, WeightedProposer,
};
use xen_rust_backend::cluster::weighted_quorum::MembershipChange;

fn deliver_prepare(
    proposer: &mut WeightedProposer,
    acceptor: &mut WeightedAcceptor,
    membership: &MembershipQuorum,
) -> PromiseProgress {
    let reply = acceptor
        .handle_prepare(proposer.prepare_request(), membership)
        .unwrap();
    proposer.handle_promise(reply).unwrap()
}

fn deliver_accept(
    proposer: &mut WeightedProposer,
    acceptor: &mut WeightedAcceptor,
    request: &AcceptRequest,
    membership: &MembershipQuorum,
) -> AcceptProgress {
    let reply = acceptor.handle_accept(request.clone(), membership).unwrap();
    proposer.handle_accepted(reply).unwrap()
}

#[test]
fn proposer_needs_weighted_prepare_and_accept_quorums_before_choice() {
    let membership = stable_quorum(&[("a", 2), ("b", 1), ("c", 1)]);
    let mut proposer = WeightedProposer::new(
        1,
        Ballot::new(1, voter("a")),
        b"requested".to_vec(),
        membership.clone(),
    )
    .unwrap();
    let mut a = WeightedAcceptor::new(voter("a"));
    let mut b = WeightedAcceptor::new(voter("b"));

    assert_eq!(
        deliver_prepare(&mut proposer, &mut a, &membership),
        PromiseProgress::Pending
    );
    // A=2 and B=1 reach the strict threshold 3; C is not required.
    let request = match deliver_prepare(&mut proposer, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        progress => panic!("expected accept request, got {progress:?}"),
    };
    assert_eq!(request.value, b"requested");
    assert_eq!(
        deliver_accept(&mut proposer, &mut a, &request, &membership),
        AcceptProgress::Pending
    );
    let chosen = match deliver_accept(&mut proposer, &mut b, &request, &membership) {
        AcceptProgress::Chosen(chosen) => chosen,
        progress => panic!("expected chosen value, got {progress:?}"),
    };
    assert_eq!(chosen.value, b"requested");
    assert_eq!(chosen.acceptors.len(), 2);
    assert_eq!(proposer.chosen_value(), Some(&chosen));
}

#[test]
fn duplicate_prepare_and_accept_replies_count_once() {
    let membership = stable_quorum(&[("a", 2), ("b", 1), ("c", 1)]);
    let mut proposer = WeightedProposer::new(
        1,
        Ballot::new(1, voter("a")),
        b"x".to_vec(),
        membership.clone(),
    )
    .unwrap();
    let mut a = WeightedAcceptor::new(voter("a"));
    let a_reply = a
        .handle_prepare(proposer.prepare_request(), &membership)
        .unwrap();
    assert_eq!(
        proposer.handle_promise(a_reply.clone()).unwrap(),
        PromiseProgress::Pending
    );
    assert_eq!(
        proposer.handle_promise(a_reply).unwrap(),
        PromiseProgress::Pending
    );
    assert!(proposer.chosen_value().is_none());

    let mut b = WeightedAcceptor::new(voter("b"));
    let accept = match deliver_prepare(&mut proposer, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected accept request, got {other:?}"),
    };
    let a_ack = a.handle_accept(accept.clone(), &membership).unwrap();
    assert_eq!(
        proposer.handle_accepted(a_ack.clone()).unwrap(),
        AcceptProgress::Pending
    );
    assert_eq!(
        proposer.handle_accepted(a_ack).unwrap(),
        AcceptProgress::Pending
    );
    assert!(proposer.chosen_value().is_none());
    assert!(matches!(
        proposer
            .handle_accepted(b.handle_accept(accept, &membership).unwrap())
            .unwrap(),
        AcceptProgress::Chosen(_)
    ));
}

#[test]
fn higher_ballot_recovers_highest_accepted_value_instead_of_overwriting_it() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let prior_ballot = Ballot::new(1, voter("a"));
    let mut a = WeightedAcceptor::new(voter("a"));
    a.handle_accept(
        AcceptRequest {
            slot: 1,
            ballot: prior_ballot.clone(),
            value: b"first-value".to_vec(),
        },
        &membership,
    )
    .unwrap();

    let mut proposer = WeightedProposer::new(
        1,
        Ballot::new(2, voter("c")),
        b"different-client-request".to_vec(),
        membership.clone(),
    )
    .unwrap();
    let mut b = WeightedAcceptor::new(voter("b"));
    assert_eq!(
        deliver_prepare(&mut proposer, &mut a, &membership),
        PromiseProgress::Pending
    );
    let accept = match deliver_prepare(&mut proposer, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected accept request, got {other:?}"),
    };
    assert_eq!(accept.value, b"first-value");
}

#[test]
fn highest_accepted_ballot_wins_when_prepare_quorum_reports_multiple_values() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut a = WeightedAcceptor::new(voter("a"));
    let mut b = WeightedAcceptor::new(voter("b"));
    a.handle_accept(
        AcceptRequest {
            slot: 1,
            ballot: Ballot::new(1, voter("a")),
            value: b"older-value".to_vec(),
        },
        &membership,
    )
    .unwrap();
    b.handle_accept(
        AcceptRequest {
            slot: 1,
            ballot: Ballot::new(2, voter("b")),
            value: b"newer-value".to_vec(),
        },
        &membership,
    )
    .unwrap();

    let mut proposer = WeightedProposer::new(
        1,
        Ballot::new(3, voter("c")),
        b"unrelated-request".to_vec(),
        membership.clone(),
    )
    .unwrap();
    assert_eq!(
        deliver_prepare(&mut proposer, &mut a, &membership),
        PromiseProgress::Pending
    );
    let request = match deliver_prepare(&mut proposer, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected accept request, got {other:?}"),
    };
    assert_eq!(request.value, b"newer-value");
}

#[test]
fn concurrent_proposers_choose_the_same_value_after_a_quorum_has_accepted_it() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut a = WeightedAcceptor::new(voter("a"));
    let mut b = WeightedAcceptor::new(voter("b"));
    let mut c = WeightedAcceptor::new(voter("c"));

    let mut first = WeightedProposer::new(
        1,
        Ballot::new(1, voter("a")),
        b"value-one".to_vec(),
        membership.clone(),
    )
    .unwrap();
    assert_eq!(
        deliver_prepare(&mut first, &mut a, &membership),
        PromiseProgress::Pending
    );
    let first_accept = match deliver_prepare(&mut first, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected accept request, got {other:?}"),
    };
    assert_eq!(
        deliver_accept(&mut first, &mut a, &first_accept, &membership),
        AcceptProgress::Pending
    );
    let first_chosen = match deliver_accept(&mut first, &mut b, &first_accept, &membership) {
        AcceptProgress::Chosen(chosen) => chosen,
        other => panic!("expected chosen value, got {other:?}"),
    };
    assert_eq!(first_chosen.value, b"value-one");

    let mut second = WeightedProposer::new(
        1,
        Ballot::new(2, voter("c")),
        b"value-two".to_vec(),
        membership.clone(),
    )
    .unwrap();
    assert_eq!(
        deliver_prepare(&mut second, &mut b, &membership),
        PromiseProgress::Pending
    );
    let second_accept = match deliver_prepare(&mut second, &mut c, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected accept request, got {other:?}"),
    };
    assert_eq!(second_accept.value, first_chosen.value);
    assert_eq!(
        deliver_accept(&mut second, &mut b, &second_accept, &membership),
        AcceptProgress::Pending
    );
    let second_chosen = match deliver_accept(&mut second, &mut c, &second_accept, &membership) {
        AcceptProgress::Chosen(chosen) => chosen,
        other => panic!("expected chosen value, got {other:?}"),
    };
    assert_eq!(second_chosen.value, first_chosen.value);
}

#[test]
fn acceptor_rejects_stale_ballots_and_conflicting_value_in_same_ballot() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut acceptor = WeightedAcceptor::new(voter("a"));
    let newer = Ballot::new(3, voter("c"));
    acceptor
        .handle_prepare(
            PrepareRequest {
                slot: 1,
                ballot: newer.clone(),
            },
            &membership,
        )
        .unwrap();
    let stale = acceptor
        .handle_accept(
            AcceptRequest {
                slot: 1,
                ballot: Ballot::new(2, voter("b")),
                value: b"stale".to_vec(),
            },
            &membership,
        )
        .unwrap();
    assert!(matches!(stale, AcceptedResponse::Rejected { promised, .. } if promised == newer));
    acceptor
        .handle_accept(
            AcceptRequest {
                slot: 1,
                ballot: newer.clone(),
                value: b"first".to_vec(),
            },
            &membership,
        )
        .unwrap();
    assert_eq!(
        acceptor.handle_accept(
            AcceptRequest {
                slot: 1,
                ballot: newer.clone(),
                value: b"second".to_vec(),
            },
            &membership,
        ),
        Err(PaxosError::ConflictingAcceptedValue(newer))
    );
}

#[test]
fn higher_promise_preempts_proposer_and_late_responses_are_ignored() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let ballot = Ballot::new(1, voter("a"));
    let higher = Ballot::new(2, voter("b"));
    let mut proposer =
        WeightedProposer::new(1, ballot.clone(), b"request".to_vec(), membership).unwrap();

    assert_eq!(
        proposer
            .handle_promise(PromiseResponse::Rejected {
                slot: 1,
                ballot: ballot.clone(),
                voter: voter("b"),
                promised: higher.clone(),
            })
            .unwrap(),
        PromiseProgress::Preempted(higher)
    );
    assert_eq!(
        proposer
            .handle_promise(PromiseResponse::Promised {
                slot: 1,
                ballot,
                voter: voter("c"),
                accepted: None,
            })
            .unwrap(),
        PromiseProgress::Ignored
    );
}

#[test]
fn chosen_certificate_requires_a_quorum_and_conflicts_are_rejected() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let chosen = ChosenValue {
        slot: 1,
        ballot: Ballot::new(1, voter("a")),
        value: b"selected".to_vec(),
        acceptors: [voter("a"), voter("b")].into_iter().collect(),
    };
    let mut a = WeightedAcceptor::new(voter("a"));
    a.handle_learn(chosen.clone(), &membership).unwrap();
    assert_eq!(a.chosen(1), Some(&chosen));

    let mut c = WeightedAcceptor::new(voter("c"));
    let insufficient = ChosenValue {
        acceptors: [voter("a")].into_iter().collect(),
        ..chosen.clone()
    };
    assert_eq!(
        c.handle_learn(insufficient, &membership),
        Err(PaxosError::InsufficientQuorum)
    );
    let conflicting = ChosenValue {
        value: b"other".to_vec(),
        ..chosen
    };
    assert_eq!(
        a.handle_learn(conflicting, &membership),
        Err(PaxosError::ConflictingChosenValue(1))
    );
}

#[test]
fn joint_membership_requires_each_side_of_quorum_for_choice() {
    let old = common::membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let joint = old
        .begin_change(MembershipChange::UpdateWeight {
            id: voter("b"),
            new_weight: 4,
        })
        .unwrap();
    let membership = MembershipQuorum::Joint(joint);
    let mut proposer = WeightedProposer::new(
        1,
        Ballot::new(1, voter("a")),
        b"value".to_vec(),
        membership.clone(),
    )
    .unwrap();
    let mut a = WeightedAcceptor::new(voter("a"));
    let mut c = WeightedAcceptor::new(voter("c"));
    let mut b = WeightedAcceptor::new(voter("b"));

    assert_eq!(
        deliver_prepare(&mut proposer, &mut a, &membership),
        PromiseProgress::Pending
    );
    assert_eq!(
        deliver_prepare(&mut proposer, &mut c, &membership),
        PromiseProgress::Pending
    );
    let request = match deliver_prepare(&mut proposer, &mut b, &membership) {
        PromiseProgress::SendAccept(request) => request,
        other => panic!("expected joint quorum, got {other:?}"),
    };
    assert_eq!(
        deliver_accept(&mut proposer, &mut a, &request, &membership),
        AcceptProgress::Pending
    );
    assert_eq!(
        deliver_accept(&mut proposer, &mut c, &request, &membership),
        AcceptProgress::Pending
    );
    assert!(matches!(
        deliver_accept(&mut proposer, &mut b, &request, &membership),
        AcceptProgress::Chosen(_)
    ));
}

#[test]
fn workers_and_zero_based_slots_are_rejected() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    assert!(matches!(
        WeightedProposer::new(
            1,
            Ballot::new(1, voter("worker")),
            vec![],
            membership.clone()
        ),
        Err(PaxosError::InvalidVoter(_))
    ));
    assert_eq!(
        WeightedProposer::new(0, Ballot::new(1, voter("a")), vec![], membership),
        Err(PaxosError::InvalidSlot)
    );
}

#[test]
fn responses_for_another_slot_or_ballot_are_not_counted() {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let ballot = Ballot::new(1, voter("a"));
    let mut proposer =
        WeightedProposer::new(1, ballot.clone(), b"value".to_vec(), membership).unwrap();

    assert_eq!(
        proposer.handle_promise(PromiseResponse::Promised {
            slot: 2,
            ballot: ballot.clone(),
            voter: voter("b"),
            accepted: None,
        }),
        Err(PaxosError::WrongSlot {
            expected: 1,
            actual: 2,
        })
    );
    assert_eq!(
        proposer.handle_promise(PromiseResponse::Promised {
            slot: 1,
            ballot: Ballot::new(2, voter("a")),
            voter: voter("b"),
            accepted: None,
        }),
        Err(PaxosError::WrongBallot)
    );
}
