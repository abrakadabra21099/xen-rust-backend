mod common;

use common::{membership, voter};
use xen_rust_backend::cluster::membership_commit::{
    MembershipCommitError, MembershipCommitTracker,
};
use xen_rust_backend::cluster::membership_state::{
    MembershipLogEntry, MembershipQuorum, MembershipState, MembershipStateError,
    MembershipStateMachine,
};
use xen_rust_backend::cluster::weighted_quorum::MembershipChange;

#[test]
fn state_machine_requires_enter_joint_then_finalize() {
    let initial = membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut machine = MembershipStateMachine::new(initial.clone());
    let change = MembershipChange::AddVoter {
        id: voter("d"),
        weight: 2,
    };
    let enter = MembershipLogEntry::EnterJoint(change);

    assert!(matches!(
        machine.quorum_for(&enter).unwrap(),
        MembershipQuorum::Joint(_)
    ));
    machine.apply_committed(enter).unwrap();
    assert!(matches!(machine.state(), MembershipState::Joint(_)));
    assert_eq!(
        machine.apply_committed(MembershipLogEntry::EnterJoint(
            MembershipChange::UpdateWeight {
                id: voter("a"),
                new_weight: 3
            }
        )),
        Err(MembershipStateError::TransitionAlreadyInProgress)
    );
    machine
        .apply_committed(MembershipLogEntry::FinalizeJoint)
        .unwrap();
    match machine.state() {
        MembershipState::Stable(current) => {
            assert_eq!(current.voter_count(), 4);
            assert_eq!(current.weight_of(&voter("d")), Some(2));
        }
        MembershipState::Joint(_) => panic!("finalize must leave joint mode"),
    }
    assert_eq!(
        machine.apply_committed(MembershipLogEntry::FinalizeJoint),
        Err(MembershipStateError::NoTransitionInProgress)
    );
}

#[test]
fn invalid_committed_change_does_not_mutate_membership() {
    let initial = membership(&[("a", 1), ("b", 1)]);
    let mut machine = MembershipStateMachine::new(initial.clone());

    let result =
        machine.apply_committed(MembershipLogEntry::EnterJoint(MembershipChange::AddVoter {
            id: voter("a"),
            weight: 5,
        }));
    assert!(matches!(
        result,
        Err(MembershipStateError::InvalidChange(_))
    ));
    assert_eq!(machine.state(), &MembershipState::Stable(initial));
}

#[test]
fn commit_tracker_waits_for_joint_quorum_and_commits_in_order() {
    let initial = membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut tracker = MembershipCommitTracker::new(initial);
    let index = tracker
        .append(MembershipLogEntry::EnterJoint(MembershipChange::AddVoter {
            id: voter("d"),
            weight: 2,
        }))
        .unwrap();
    assert_eq!(index, 1);
    assert_eq!(
        tracker.acknowledge(index, voter("a")).unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(
        tracker.acknowledge(index, voter("b")).unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(tracker.commit_index(), 0);
    assert_eq!(
        tracker.append(MembershipLogEntry::FinalizeJoint),
        Err(MembershipCommitError::PendingRecord)
    );
    assert_eq!(tracker.acknowledge(index, voter("d")).unwrap(), vec![1]);
    assert_eq!(tracker.commit_index(), 1);

    let finalize = tracker.append(MembershipLogEntry::FinalizeJoint).unwrap();
    assert_eq!(
        tracker.acknowledge(finalize, voter("a")).unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(
        tracker.acknowledge(finalize, voter("b")).unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(tracker.acknowledge(finalize, voter("d")).unwrap(), vec![2]);
    assert_eq!(tracker.commit_index(), 2);
    assert!(matches!(tracker.state(), MembershipState::Stable(_)));
}

#[test]
fn commit_tracker_counts_duplicate_ack_once_and_rejects_unknown_nodes() {
    let mut tracker = MembershipCommitTracker::new(membership(&[("a", 1), ("b", 1), ("c", 1)]));
    let index = tracker
        .append(MembershipLogEntry::EnterJoint(
            MembershipChange::UpdateWeight {
                id: voter("a"),
                new_weight: 2,
            },
        ))
        .unwrap();

    assert!(matches!(
        tracker.acknowledge(index, voter("worker")),
        Err(MembershipCommitError::InvalidVoter(_))
    ));
    assert!(tracker.acknowledge(index, voter("a")).unwrap().is_empty());
    assert!(tracker.acknowledge(index, voter("a")).unwrap().is_empty());
    assert_eq!(tracker.commit_index(), 0);
    assert_eq!(tracker.acknowledge(index, voter("b")).unwrap(), vec![1]);
}

#[test]
fn commit_tracker_rejects_acknowledgements_for_unknown_indexes() {
    let mut tracker = MembershipCommitTracker::new(membership(&[("a", 1), ("b", 1)]));

    assert_eq!(
        tracker.acknowledge(1, voter("a")),
        Err(MembershipCommitError::UnknownRecord(1))
    );
}
