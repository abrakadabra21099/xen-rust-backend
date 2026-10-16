//! Durable SQLite storage for Paxos acceptor state and proposer ballot rounds.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use super::paxos::{AcceptedValue, AcceptorSlot, Ballot, ChosenValue, PaxosError, PaxosSlot};
use super::weighted_quorum::VoterId;

/// SQLite storage failures and protocol validation errors from a transaction.
#[derive(Debug)]
pub enum PaxosStorageError {
    Sqlite(rusqlite::Error),
    Protocol(PaxosError),
    CorruptState(String),
    BallotRoundOverflow,
    UnsupportedSchemaVersion(i64),
    InvalidReadLimit,
    CursorBeyondCommitIndex {
        cursor: PaxosSlot,
        commit_index: PaxosSlot,
    },
    MissingCommittedValue(PaxosSlot),
}

impl std::fmt::Display for PaxosStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "SQLite error: {error}"),
            Self::Protocol(error) => write!(f, "Paxos request rejected: {error}"),
            Self::CorruptState(message) => write!(f, "corrupt Paxos storage state: {message}"),
            Self::BallotRoundOverflow => write!(f, "Paxos ballot round exceeds u64"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(f, "unsupported Paxos storage schema version: {version}")
            }
            Self::InvalidReadLimit => write!(f, "committed log read limit must be positive"),
            Self::CursorBeyondCommitIndex {
                cursor,
                commit_index,
            } => write!(
                f,
                "replay cursor {cursor} is beyond commit index {commit_index}"
            ),
            Self::MissingCommittedValue(slot) => {
                write!(f, "committed log slot {slot} has no learned value")
            }
        }
    }
}

impl std::error::Error for PaxosStorageError {}

impl From<rusqlite::Error> for PaxosStorageError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<PaxosError> for PaxosStorageError {
    fn from(error: PaxosError) -> Self {
        Self::Protocol(error)
    }
}

/// One SQLite database belongs to one control-plane node; databases are not
/// shared between cluster members. Each positive acceptor response follows a
/// committed `synchronous=FULL` transaction.
pub struct SqlitePaxosStore {
    connection: Connection,
}

impl SqlitePaxosStore {
    /// Opens or creates a node-local database and initializes its schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PaxosStorageError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;

        let schema_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match schema_version {
            0 => {
                connection.execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE acceptor_slots (
                         slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot) = 8),
                         promised_round BLOB CHECK(promised_round IS NULL OR length(promised_round) = 8),
                         promised_proposer TEXT,
                         accepted_round BLOB CHECK(accepted_round IS NULL OR length(accepted_round) = 8),
                         accepted_proposer TEXT,
                         accepted_value BLOB,
                         chosen_round BLOB CHECK(chosen_round IS NULL OR length(chosen_round) = 8),
                         chosen_proposer TEXT,
                         chosen_value BLOB,
                         CHECK((promised_round IS NULL) = (promised_proposer IS NULL)),
                         CHECK((accepted_round IS NULL) = (accepted_proposer IS NULL)),
                         CHECK((accepted_round IS NULL) = (accepted_value IS NULL)),
                         CHECK((chosen_round IS NULL) = (chosen_proposer IS NULL)),
                         CHECK((chosen_round IS NULL) = (chosen_value IS NULL))
                     );
                     CREATE TABLE chosen_acceptors (
                         slot BLOB NOT NULL REFERENCES acceptor_slots(slot) ON DELETE CASCADE,
                         voter_id TEXT NOT NULL,
                         PRIMARY KEY(slot, voter_id)
                     );
                     CREATE TABLE ballot_counters (
                         proposer TEXT PRIMARY KEY NOT NULL,
                         last_round BLOB NOT NULL CHECK(length(last_round) = 8)
                     );
                     CREATE TABLE consensus_meta (
                         singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                         commit_index BLOB NOT NULL CHECK(length(commit_index) = 8)
                     );
                     INSERT INTO consensus_meta(singleton, commit_index)
                         VALUES (1, X'0000000000000000');
                     PRAGMA user_version = 2;
                     COMMIT;",
                )?;
            }
            1 => {
                connection.execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE consensus_meta (
                         singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                         commit_index BLOB NOT NULL CHECK(length(commit_index) = 8)
                     );
                     INSERT INTO consensus_meta(singleton, commit_index)
                         VALUES (1, X'0000000000000000');
                     PRAGMA user_version = 2;
                     COMMIT;",
                )?;
            }
            2 => {}
            other => return Err(PaxosStorageError::UnsupportedSchemaVersion(other)),
        }

        let mut store = Self { connection };
        let transaction = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        advance_commit_index(&transaction)?;
        transaction.commit()?;
        Ok(store)
    }

    /// Returns the durable next round for a proposer, persisting it atomically.
    ///
    /// Callers must use the returned round in the ballot and must not reuse it
    /// for another proposal after a restart.
    pub fn next_ballot_round(&mut self, proposer: &VoterId) -> Result<u64, PaxosStorageError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT last_round FROM ballot_counters WHERE proposer = ?1",
                [proposer.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let round = match previous {
            Some(encoded) => decode_u64(encoded)?
                .checked_add(1)
                .ok_or(PaxosStorageError::BallotRoundOverflow)?,
            None => 1,
        };
        transaction.execute(
            "INSERT INTO ballot_counters(proposer, last_round) VALUES (?1, ?2)
             ON CONFLICT(proposer) DO UPDATE SET last_round = excluded.last_round",
            params![proposer.as_str(), encode_u64(round).as_slice()],
        )?;
        transaction.commit()?;
        Ok(round)
    }

    pub(crate) fn load_acceptor_slots(
        &self,
    ) -> Result<BTreeMap<PaxosSlot, AcceptorSlot>, PaxosStorageError> {
        let mut statement = self.connection.prepare("SELECT slot FROM acceptor_slots")?;
        let slots = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        slots
            .into_iter()
            .map(|encoded_slot| {
                let slot = decode_u64(encoded_slot)?;
                Ok((slot, load_slot(&self.connection, slot)?))
            })
            .collect()
    }

    pub(crate) fn update_slot<T>(
        &mut self,
        slot: PaxosSlot,
        update: impl FnOnce(&mut AcceptorSlot) -> Result<T, PaxosError>,
    ) -> Result<(T, AcceptorSlot), PaxosStorageError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut state = load_slot(&transaction, slot)?;
        let result = update(&mut state)?;
        save_slot(&transaction, slot, &state)?;
        if state.chosen.is_some() {
            advance_commit_index(&transaction)?;
        }
        transaction.commit()?;
        Ok((result, state))
    }

    /// Highest contiguous, durably learned log slot. Slots begin at index 1.
    pub fn commit_index(&self) -> Result<PaxosSlot, PaxosStorageError> {
        let encoded: Vec<u8> = self.connection.query_row(
            "SELECT commit_index FROM consensus_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        decode_u64(encoded)
    }

    /// Returns a learned value for recovery/replay, if this slot is known.
    pub fn chosen_value(&self, slot: PaxosSlot) -> Result<Option<ChosenValue>, PaxosStorageError> {
        if slot == 0 {
            return Err(PaxosError::InvalidSlot.into());
        }
        Ok(load_slot(&self.connection, slot)?.chosen)
    }

    /// Reads learned values after `applied_index` in log order, bounded by `limit`.
    ///
    /// The cursor must not be ahead of the durable commit index. A missing value
    /// inside the committed prefix indicates storage corruption and is reported
    /// instead of silently skipping a log entry.
    pub fn committed_values_after(
        &self,
        applied_index: PaxosSlot,
        limit: usize,
    ) -> Result<Vec<ChosenValue>, PaxosStorageError> {
        if limit == 0 {
            return Err(PaxosStorageError::InvalidReadLimit);
        }

        let commit_index = self.commit_index()?;
        if applied_index > commit_index {
            return Err(PaxosStorageError::CursorBeyondCommitIndex {
                cursor: applied_index,
                commit_index,
            });
        }

        let mut entries = Vec::new();
        let mut slot = applied_index;
        while entries.len() < limit && slot < commit_index {
            slot += 1;
            let chosen = self
                .chosen_value(slot)?
                .ok_or(PaxosStorageError::MissingCommittedValue(slot))?;
            entries.push(chosen);
        }
        Ok(entries)
    }
}

fn advance_commit_index(transaction: &Transaction<'_>) -> Result<PaxosSlot, PaxosStorageError> {
    let encoded: Vec<u8> = transaction.query_row(
        "SELECT commit_index FROM consensus_meta WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let mut commit_index = decode_u64(encoded)?;

    loop {
        let Some(next) = commit_index.checked_add(1) else {
            break;
        };
        let encoded_next = encode_u64(next);
        let is_chosen: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM acceptor_slots
                WHERE slot = ?1 AND chosen_round IS NOT NULL
             )",
            [encoded_next.as_slice()],
            |row| row.get(0),
        )?;
        if !is_chosen {
            break;
        }
        commit_index = next;
    }

    transaction.execute(
        "UPDATE consensus_meta SET commit_index = ?1 WHERE singleton = 1",
        [encode_u64(commit_index).as_slice()],
    )?;
    Ok(commit_index)
}

fn load_slot(connection: &Connection, slot: PaxosSlot) -> Result<AcceptorSlot, PaxosStorageError> {
    let encoded_slot = encode_u64(slot);
    let row: Option<(
        Option<Vec<u8>>,
        Option<String>,
        Option<Vec<u8>>,
        Option<String>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<String>,
        Option<Vec<u8>>,
    )> = connection
        .query_row(
            "SELECT promised_round, promised_proposer, accepted_round,
                    accepted_proposer, accepted_value, chosen_round,
                    chosen_proposer, chosen_value
             FROM acceptor_slots WHERE slot = ?1",
            [encoded_slot.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        promised_round,
        promised_proposer,
        accepted_round,
        accepted_proposer,
        accepted_value,
        chosen_round,
        chosen_proposer,
        chosen_value,
    )) = row
    else {
        return Ok(AcceptorSlot::default());
    };

    let promised = decode_ballot(promised_round, promised_proposer)?;
    let accepted_ballot = decode_ballot(accepted_round, accepted_proposer)?;
    let accepted = match (accepted_ballot, accepted_value) {
        (Some(ballot), Some(value)) => Some(AcceptedValue { ballot, value }),
        (None, None) => None,
        _ => {
            return Err(PaxosStorageError::CorruptState(
                "partial accepted value".into(),
            ))
        }
    };

    let chosen_ballot = decode_ballot(chosen_round, chosen_proposer)?;
    let chosen = match (chosen_ballot, chosen_value) {
        (Some(ballot), Some(value)) => {
            let mut statement = connection.prepare(
                "SELECT voter_id FROM chosen_acceptors WHERE slot = ?1 ORDER BY voter_id",
            )?;
            let acceptors = statement
                .query_map([encoded_slot.as_slice()], |row| {
                    let id: String = row.get(0)?;
                    VoterId::new(id).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })
                })?
                .collect::<Result<BTreeSet<_>, _>>()?;
            Some(ChosenValue {
                slot,
                ballot,
                value,
                acceptors,
            })
        }
        (None, None) => None,
        _ => {
            return Err(PaxosStorageError::CorruptState(
                "partial chosen value".into(),
            ))
        }
    };

    Ok(AcceptorSlot {
        promised,
        accepted,
        chosen,
    })
}

fn save_slot(
    transaction: &Transaction<'_>,
    slot: PaxosSlot,
    state: &AcceptorSlot,
) -> Result<(), PaxosStorageError> {
    let encoded_slot = encode_u64(slot);
    let (promised_round, promised_proposer) = encode_optional_ballot(state.promised.as_ref());
    let (accepted_round, accepted_proposer, accepted_value) = match &state.accepted {
        Some(accepted) => (
            Some(encode_u64(accepted.ballot.round())),
            Some(accepted.ballot.proposer().as_str()),
            Some(accepted.value.as_slice()),
        ),
        None => (None, None, None),
    };
    let (chosen_round, chosen_proposer, chosen_value) = match &state.chosen {
        Some(chosen) => (
            Some(encode_u64(chosen.ballot.round())),
            Some(chosen.ballot.proposer().as_str()),
            Some(chosen.value.as_slice()),
        ),
        None => (None, None, None),
    };

    transaction.execute(
        "INSERT INTO acceptor_slots(
             slot, promised_round, promised_proposer, accepted_round,
             accepted_proposer, accepted_value, chosen_round, chosen_proposer,
             chosen_value
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(slot) DO UPDATE SET
             promised_round = excluded.promised_round,
             promised_proposer = excluded.promised_proposer,
             accepted_round = excluded.accepted_round,
             accepted_proposer = excluded.accepted_proposer,
             accepted_value = excluded.accepted_value,
             chosen_round = excluded.chosen_round,
             chosen_proposer = excluded.chosen_proposer,
             chosen_value = excluded.chosen_value",
        params![
            encoded_slot.as_slice(),
            promised_round.as_deref(),
            promised_proposer,
            accepted_round.as_ref().map(|value| value.as_slice()),
            accepted_proposer,
            accepted_value,
            chosen_round.as_ref().map(|value| value.as_slice()),
            chosen_proposer,
            chosen_value,
        ],
    )?;
    transaction.execute(
        "DELETE FROM chosen_acceptors WHERE slot = ?1",
        [encoded_slot.as_slice()],
    )?;
    if let Some(chosen) = &state.chosen {
        for acceptor in &chosen.acceptors {
            transaction.execute(
                "INSERT INTO chosen_acceptors(slot, voter_id) VALUES (?1, ?2)",
                params![encoded_slot.as_slice(), acceptor.as_str()],
            )?;
        }
    }
    Ok(())
}

fn decode_ballot(
    round: Option<Vec<u8>>,
    proposer: Option<String>,
) -> Result<Option<Ballot>, PaxosStorageError> {
    match (round, proposer) {
        (Some(round), Some(proposer)) => Ok(Some(Ballot::new(
            decode_u64(round)?,
            VoterId::new(proposer)
                .map_err(|error| PaxosStorageError::CorruptState(error.to_string()))?,
        ))),
        (None, None) => Ok(None),
        _ => Err(PaxosStorageError::CorruptState("partial ballot".into())),
    }
}

fn encode_optional_ballot(ballot: Option<&Ballot>) -> (Option<Vec<u8>>, Option<&str>) {
    ballot.map_or((None, None), |ballot| {
        (
            Some(encode_u64(ballot.round()).to_vec()),
            Some(ballot.proposer().as_str()),
        )
    })
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(bytes: Vec<u8>) -> Result<u64, PaxosStorageError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        PaxosStorageError::CorruptState("integer field must contain exactly 8 bytes".into())
    })?;
    Ok(u64::from_be_bytes(bytes))
}
