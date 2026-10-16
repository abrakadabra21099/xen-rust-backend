mod common;

use common::{membership, voter};
use xen_rust_backend::cluster::weighted_quorum::{
    MembershipChange, QuorumConfigError, WeightedMembership,
};

#[test]
fn strict_weighted_majority_uses_total_configured_weight() {
    let config = membership(&[("a", 4), ("b", 2), ("c", 1)]);

    assert_eq!(config.total_weight(), 7);
    assert_eq!(config.quorum_threshold(), 4);
    assert!(config.has_quorum([voter("a")]).unwrap());
    assert!(!config.has_quorum([voter("b"), voter("c")]).unwrap());
}

#[test]
fn exactly_half_of_an_even_total_is_not_a_quorum() {
    let config = membership(&[("a", 1), ("b", 1), ("c", 1), ("d", 1)]);

    assert_eq!(config.quorum_threshold(), 3);
    assert!(!config.has_quorum([voter("a"), voter("b")]).unwrap());
    assert!(config
        .has_quorum([voter("a"), voter("b"), voter("c")])
        .unwrap());
}

#[test]
fn duplicate_acknowledgements_do_not_add_weight() {
    let config = membership(&[("a", 2), ("b", 1), ("c", 1)]);

    assert!(!config
        .has_quorum([voter("a"), voter("a"), voter("a")])
        .unwrap());
    assert!(config.has_quorum([voter("a"), voter("b")]).unwrap());
}

#[test]
fn unknown_members_cannot_contribute_to_quorum() {
    let config = membership(&[("a", 1), ("b", 1), ("c", 1)]);

    assert_eq!(
        config.has_quorum([voter("worker")]),
        Err(QuorumConfigError::UnknownVoter(voter("worker")))
    );
}

#[test]
fn invalid_membership_definitions_are_rejected() {
    assert_eq!(
        WeightedMembership::new(Vec::<(
            xen_rust_backend::cluster::weighted_quorum::VoterId,
            u64
        )>::new()),
        Err(QuorumConfigError::EmptyMembership)
    );
    assert_eq!(
        WeightedMembership::new([(voter("a"), 0)]),
        Err(QuorumConfigError::ZeroWeight(voter("a")))
    );
    assert_eq!(
        WeightedMembership::new([(voter("a"), 1), (voter("a"), 2)]),
        Err(QuorumConfigError::DuplicateVoter(voter("a")))
    );
    assert_eq!(
        WeightedMembership::new([(voter("a"), u64::MAX), (voter("b"), 1)]),
        Err(QuorumConfigError::TotalWeightOverflow)
    );
}

#[test]
fn explicit_weight_update_returns_new_membership_without_mutating_old_one() {
    let old = membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let updated = old.with_updated_weight(&voter("b"), 4).unwrap();

    assert_eq!(old.weight_of(&voter("b")), Some(1));
    assert_eq!(updated.weight_of(&voter("b")), Some(4));
    assert_eq!(updated.total_weight(), 6);
    assert_eq!(updated.quorum_threshold(), 4);
}

#[test]
fn membership_changes_validate_add_update_remove_and_last_voter() {
    let old = membership(&[("a", 2), ("b", 1)]);

    let add = old
        .begin_change(MembershipChange::AddVoter {
            id: voter("c"),
            weight: 3,
        })
        .unwrap();
    assert_eq!(add.old(), &old);
    assert_eq!(add.new_membership().weight_of(&voter("c")), Some(3));
    assert_eq!(
        old.begin_change(MembershipChange::AddVoter {
            id: voter("a"),
            weight: 1
        }),
        Err(QuorumConfigError::VoterAlreadyExists(voter("a")))
    );
    assert_eq!(
        old.begin_change(MembershipChange::UpdateWeight {
            id: voter("absent"),
            new_weight: 2
        }),
        Err(QuorumConfigError::UnknownVoter(voter("absent")))
    );
    assert_eq!(
        old.begin_change(MembershipChange::AddVoter {
            id: voter("d"),
            weight: 0,
        }),
        Err(QuorumConfigError::ZeroWeight(voter("d")))
    );
    assert_eq!(
        old.begin_change(MembershipChange::RemoveVoter {
            id: voter("absent"),
        }),
        Err(QuorumConfigError::UnknownVoter(voter("absent")))
    );
    assert_eq!(
        old.begin_change(MembershipChange::RemoveVoter { id: voter("a") })
            .unwrap()
            .new_membership()
            .total_weight(),
        1
    );
    assert_eq!(
        membership(&[("only", 1)])
            .begin_change(MembershipChange::RemoveVoter { id: voter("only") }),
        Err(QuorumConfigError::CannotRemoveLastVoter)
    );
}

#[test]
fn joint_quorum_must_satisfy_both_old_and_new_weighted_majorities() {
    let old = membership(&[("a", 1), ("b", 1), ("c", 1)]);
    let joint = old
        .begin_change(MembershipChange::UpdateWeight {
            id: voter("b"),
            new_weight: 4,
        })
        .unwrap();

    assert!(joint.has_quorum([voter("a"), voter("b")]).unwrap());
    assert!(!joint.has_quorum([voter("a"), voter("c")]).unwrap());
    assert_eq!(
        joint.has_quorum([voter("worker")]),
        Err(QuorumConfigError::UnknownVoter(voter("worker")))
    );
}
