mod common;

use common::{stable_quorum, voter, TempDb};
use xen_rust_backend::cluster::{
    application::{ApplicationError, CommittedLogApplier, ReplicatedStateMachine},
    paxos::{Ballot, ChosenValue, WeightedAcceptor},
    paxos_storage::SqlitePaxosStore,
};

struct RecordingState {
    applied: Vec<(u64, Vec<u8>)>,
    fail_once_at: Option<u64>,
}

impl ReplicatedStateMachine for RecordingState {
    fn apply(&mut self, slot: u64, value: &[u8]) -> Result<(), String> {
        if self.fail_once_at == Some(slot) {
            self.fail_once_at = None;
            return Err("temporary application failure".into());
        }
        self.applied.push((slot, value.to_vec()));
        Ok(())
    }
}

fn chosen(slot: u64, value: &[u8]) -> ChosenValue {
    ChosenValue {
        slot,
        ballot: Ballot::new(slot, voter("a")),
        value: value.to_vec(),
        acceptors: [voter("a"), voter("b")].into_iter().collect(),
    }
}

fn seed_log(store: &mut SqlitePaxosStore) {
    let membership = stable_quorum(&[("a", 1), ("b", 1), ("c", 1)]);
    let mut acceptor = WeightedAcceptor::new(voter("a"));
    for (slot, value) in [(1, b"first".as_slice()), (2, b"second"), (3, b"third")] {
        acceptor
            .handle_learn_durable(chosen(slot, value), &membership, store)
            .unwrap();
    }
}

#[test]
fn applier_delivers_committed_values_in_order_and_persists_batches() {
    let db = TempDb::new();
    {
        let mut store = SqlitePaxosStore::open(db.path()).unwrap();
        seed_log(&mut store);
        let mut state = RecordingState {
            applied: Vec::new(),
            fail_once_at: None,
        };

        assert_eq!(
            CommittedLogApplier::apply_available(&mut store, &mut state, 2).unwrap(),
            2
        );
        assert_eq!(store.applied_index().unwrap(), 2);
        assert_eq!(
            state.applied,
            vec![(1, b"first".to_vec()), (2, b"second".to_vec())]
        );
    }

    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut state = RecordingState {
        applied: Vec::new(),
        fail_once_at: None,
    };
    assert_eq!(
        CommittedLogApplier::apply_available(&mut store, &mut state, 2).unwrap(),
        1
    );
    assert_eq!(store.applied_index().unwrap(), 3);
    assert_eq!(state.applied, vec![(3, b"third".to_vec())]);
    assert_eq!(
        CommittedLogApplier::apply_available(&mut store, &mut state, 2).unwrap(),
        0
    );
}

#[test]
fn failed_application_does_not_advance_past_the_failed_slot_and_retries_it() {
    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    seed_log(&mut store);
    let mut state = RecordingState {
        applied: Vec::new(),
        fail_once_at: Some(2),
    };

    assert!(matches!(
        CommittedLogApplier::apply_available(&mut store, &mut state, 3),
        Err(ApplicationError::Apply { slot: 2, .. })
    ));
    assert_eq!(store.applied_index().unwrap(), 1);
    assert_eq!(state.applied, vec![(1, b"first".to_vec())]);

    assert_eq!(
        CommittedLogApplier::apply_available(&mut store, &mut state, 3).unwrap(),
        2
    );
    assert_eq!(store.applied_index().unwrap(), 3);
    assert_eq!(
        state.applied,
        vec![
            (1, b"first".to_vec()),
            (2, b"second".to_vec()),
            (3, b"third".to_vec())
        ]
    );
}

#[test]
fn applier_rejects_zero_batch_limit_without_calling_state_machine() {
    let db = TempDb::new();
    let mut store = SqlitePaxosStore::open(db.path()).unwrap();
    let mut state = RecordingState {
        applied: Vec::new(),
        fail_once_at: None,
    };

    assert!(matches!(
        CommittedLogApplier::apply_available(&mut store, &mut state, 0),
        Err(ApplicationError::Storage(
            xen_rust_backend::cluster::paxos_storage::PaxosStorageError::InvalidReadLimit
        ))
    ));
    assert!(state.applied.is_empty());
    assert_eq!(store.applied_index().unwrap(), 0);
}
