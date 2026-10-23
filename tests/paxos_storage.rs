mod common;

use common::{stable_quorum, voter, TempDb};
use rusqlite::Connection;
use xen_rust_backend::cluster::paxos::{
    AcceptRequest, AcceptedValue, Ballot, ChosenValue, PaxosError, PrepareRequest, PromiseResponse,
    WeightedAcceptor,
};
use xen_rust_backend::cluster::paxos_storage::{PaxosStorageError, SqlitePaxosStore};

fn chosen(slot: u64, value: &[u8]) -> ChosenValue {
    ChosenValue {
        slot,
        ballot: Ballot::new(1, voter("a")),
        value: value.to_vec(),
        acceptors: [voter("a"), voter("b")].into_iter().collect(),
    }
}

#[test]
fn promised_ballot_and_accepted_value_survive_reopen() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let higher = Ballot::new(5, voter("c"));

    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        let response = acceptor
            .handle_prepare_durable(
                PrepareRequest {
                    slot: 1,
                    ballot: higher.clone(),
                },
                &membership,
                &mut store,
            )
            .unwrap();
        assert!(matches!(response, PromiseResponse::Promised { .. }));
    }

    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::restore(voter("a"), &store).unwrap();
        let stale = acceptor
            .handle_prepare_durable(
                PrepareRequest {
                    slot: 1,
                    ballot: Ballot::new(4, voter("b")),
                },
                &membership,
                &mut store,
            )
            .unwrap();
        assert!(matches!(
            stale,
            PromiseResponse::Rejected { promised, .. } if promised == higher
        ));

        let accepted_ballot = Ballot::new(6, voter("b"));
        acceptor
            .handle_prepare_durable(
                PrepareRequest {
                    slot: 1,
                    ballot: accepted_ballot.clone(),
                },
                &membership,
                &mut store,
            )
            .unwrap();
        acceptor
            .handle_accept_durable(
                AcceptRequest {
                    slot: 1,
                    ballot: accepted_ballot.clone(),
                    value: b"durable-value".to_vec(),
                },
                &membership,
                &mut store,
            )
            .unwrap();
    }

    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::restore(voter("a"), &store).unwrap();
        assert_eq!(
            acceptor.accepted(1),
            Some(&AcceptedValue {
                ballot: Ballot::new(6, voter("b")),
                value: b"durable-value".to_vec(),
            })
        );
        let promise = acceptor
            .handle_prepare_durable(
                PrepareRequest {
                    slot: 1,
                    ballot: Ballot::new(7, voter("c")),
                },
                &membership,
                &mut store,
            )
            .unwrap();
        assert!(matches!(
            promise,
            PromiseResponse::Promised { accepted: Some(AcceptedValue { value, .. }), .. }
                if value == b"durable-value"
        ));
    }
}

#[test]
fn learned_slots_advance_only_a_contiguous_durable_commit_prefix() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);

    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));

        acceptor
            .handle_learn_durable(chosen(2, b"second"), &membership, &mut store)
            .unwrap();
        assert_eq!(store.commit_index().unwrap(), 0);
        assert_eq!(store.chosen_value(2).unwrap().unwrap().value, b"second");

        acceptor
            .handle_learn_durable(chosen(1, b"first"), &membership, &mut store)
            .unwrap();
        assert_eq!(store.commit_index().unwrap(), 2);
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.commit_index().unwrap(), 2);
    assert_eq!(store.chosen_value(1).unwrap().unwrap().value, b"first");
    assert_eq!(store.chosen_value(2).unwrap().unwrap().value, b"second");
}

#[test]
fn committed_log_reads_only_the_contiguous_prefix_in_order_across_restarts() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        acceptor
            .handle_learn_durable(chosen(2, b"second"), &membership, &mut store)
            .unwrap();
        assert!(store.committed_values_after(0, 8).unwrap().is_empty());
        acceptor
            .handle_learn_durable(chosen(1, b"first"), &membership, &mut store)
            .unwrap();
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    let first_batch = store.committed_values_after(0, 1).unwrap();
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].slot, 1);
    assert_eq!(first_batch[0].value, b"first");

    let remaining = store
        .committed_values_after(first_batch[0].slot, 8)
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].slot, 2);
    assert_eq!(remaining[0].value, b"second");
    assert!(store.committed_values_after(2, 1).unwrap().is_empty());
}

#[test]
fn committed_log_reads_reject_invalid_limits_and_ahead_cursors() {
    let db = TempDb::new();
    let store = SqlitePaxosStore::open(db.path()).unwrap();

    assert!(matches!(
        store.committed_values_after(0, 0),
        Err(PaxosStorageError::InvalidReadLimit)
    ));
    assert!(matches!(
        store.committed_values_after(1, 1),
        Err(PaxosStorageError::CursorBeyondCommitIndex {
            cursor: 1,
            commit_index: 0
        })
    ));
    assert!(matches!(
        store.chosen_value(0),
        Err(PaxosStorageError::Protocol(PaxosError::InvalidSlot))
    ));
}

#[test]
fn committed_log_reader_reports_missing_data_inside_committed_prefix() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        acceptor
            .handle_learn_durable(chosen(1, b"committed"), &membership, &mut store)
            .unwrap();
    }

    let connection = Connection::open(db.path()).unwrap();
    connection
        .execute(
            "DELETE FROM acceptor_slots WHERE slot = ?1",
            [1_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
    drop(connection);

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    assert!(matches!(
        store.committed_values_after(0, 1),
        Err(PaxosStorageError::MissingCommittedValue(1))
    ));
}

#[test]
fn proposer_ballot_counter_is_monotonic_across_restarts() {
    let db = TempDb::new();
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        assert_eq!(store.next_ballot_round(&voter("node-a")).unwrap(), 1);
        assert_eq!(store.next_ballot_round(&voter("node-a")).unwrap(), 2);
        assert_eq!(store.next_ballot_round(&voter("node-b")).unwrap(), 1);
    }
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.next_ballot_round(&voter("node-a")).unwrap(), 3);
}

#[test]
fn failed_protocol_update_rolls_back_without_corrupting_saved_acceptance() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let ballot = Ballot::new(1, voter("a"));
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        acceptor
            .handle_accept_durable(
                AcceptRequest {
                    slot: 1,
                    ballot: ballot.clone(),
                    value: b"original".to_vec(),
                },
                &membership,
                &mut store,
            )
            .unwrap();
        let result = acceptor.handle_accept_durable(
            AcceptRequest {
                slot: 1,
                ballot: ballot.clone(),
                value: b"conflicting".to_vec(),
            },
            &membership,
            &mut store,
        );
        assert!(matches!(result, Err(PaxosStorageError::Protocol(_))));
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    let acceptor = WeightedAcceptor::restore(voter("a"), &store).unwrap();
    assert_eq!(acceptor.accepted(1).unwrap().value, b"original");
}

#[test]
fn schema_v1_migrates_and_rebuilds_contiguous_commit_index() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        acceptor
            .handle_learn_durable(chosen(1, b"one"), &membership, &mut store)
            .unwrap();
        acceptor
            .handle_learn_durable(chosen(2, b"two"), &membership, &mut store)
            .unwrap();
    }

    {
        let connection = Connection::open(db.path()).unwrap();
        connection.execute("DROP TABLE consensus_meta", []).unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
    }

    let migrated = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(migrated.commit_index().unwrap(), 2);
}

#[test]
fn unsupported_schema_version_is_rejected_without_reinitializing_database() {
    let db = TempDb::new();
    {
        let connection = Connection::open(db.path()).unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
    }

    assert!(matches!(
        SqlitePaxosStore::open(db.path()),
        Err(PaxosStorageError::UnsupportedSchemaVersion(99))
    ));
}

#[test]
fn durable_learn_rejects_a_certificate_below_quorum() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut acceptor = WeightedAcceptor::new(voter("a"));
    let mut insufficient = chosen(1, b"uncommitted");
    insufficient.acceptors = [voter("a")].into_iter().collect();

    assert!(matches!(
        acceptor.handle_learn_durable(insufficient, &membership, &mut store),
        Err(PaxosStorageError::Protocol(PaxosError::InsufficientQuorum))
    ));
    assert_eq!(store.commit_index().unwrap(), 0);
    assert_eq!(store.chosen_value(1).unwrap(), None);
}

#[test]
fn durable_accept_cannot_overwrite_a_previously_learned_chosen_value() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let learned = chosen(1, b"chosen");
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("c"));
        acceptor
            .handle_learn_durable(learned.clone(), &membership, &mut store)
            .unwrap();

        assert!(matches!(
            acceptor.handle_accept_durable(
                AcceptRequest {
                    slot: 1,
                    ballot: Ballot::new(2, voter("c")),
                    value: b"conflicting".to_vec(),
                },
                &membership,
                &mut store,
            ),
            Err(PaxosStorageError::Protocol(
                PaxosError::ConflictingChosenValue(1)
            ))
        ));
        assert_eq!(store.commit_index().unwrap(), 1);
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.chosen_value(1).unwrap(), Some(learned));
    let acceptor = WeightedAcceptor::restore(voter("c"), &store).unwrap();
    assert!(acceptor.accepted(1).is_none());
}

#[test]
fn ballot_round_allocation_moves_above_saved_and_observed_rounds() {
    let db = TempDb::new();
    let proposer = voter("a");
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();

    assert_eq!(store.next_ballot_round(&proposer).unwrap(), 1);
    assert_eq!(store.next_ballot_round_after(&proposer, 20).unwrap(), 21);
    assert_eq!(store.next_ballot_round_after(&proposer, 7).unwrap(), 22);
    drop(store);

    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.next_ballot_round(&proposer).unwrap(), 23);
}

#[test]
fn ballot_round_overflow_does_not_replace_the_durable_counter() {
    let db = TempDb::new();
    let proposer = voter("a");
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.next_ballot_round(&proposer).unwrap(), 1);

    assert!(matches!(
        store.next_ballot_round_after(&proposer, u64::MAX),
        Err(PaxosStorageError::BallotRoundOverflow)
    ));
    assert_eq!(store.next_ballot_round(&proposer).unwrap(), 2);
}

#[test]
fn application_cursor_advances_only_one_committed_slot_and_survives_reopen() {
    let db = TempDb::new();
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        let mut acceptor = WeightedAcceptor::new(voter("a"));
        acceptor
            .handle_learn_durable(chosen(1, b"one"), &membership, &mut store)
            .unwrap();

        assert!(matches!(
            store.mark_applied(2),
            Err(PaxosStorageError::AppliedSlotOutOfOrder {
                expected: 1,
                actual: 2
            })
        ));
        assert_eq!(store.applied_index().unwrap(), 0);
        store.mark_applied(1).unwrap();
        assert!(matches!(
            store.mark_applied(2),
            Err(PaxosStorageError::AppliedBeyondCommitIndex {
                slot: 2,
                commit_index: 1
            })
        ));
        assert_eq!(store.applied_index().unwrap(), 1);
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.applied_index().unwrap(), 1);
}

#[test]
fn schema_v2_migrates_application_cursor_to_zero() {
    let db = TempDb::new();
    {
        let _store = SqlitePaxosStore::open(db.path()).unwrap();
    }
    {
        let connection = Connection::open(db.path()).unwrap();
        connection
            .execute("ALTER TABLE consensus_meta DROP COLUMN applied_index", [])
            .unwrap();
        connection.pragma_update(None, "user_version", 2).unwrap();
    }

    let store = SqlitePaxosStore::open(db.path()).unwrap();
    assert_eq!(store.applied_index().unwrap(), 0);
}
