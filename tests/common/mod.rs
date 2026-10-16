#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use xen_rust_backend::cluster::membership_state::MembershipQuorum;
use xen_rust_backend::cluster::weighted_quorum::{VoterId, WeightedMembership};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub fn voter(id: &str) -> VoterId {
    VoterId::new(id).unwrap()
}

pub fn membership(weights: &[(&str, u64)]) -> WeightedMembership {
    WeightedMembership::new(weights.iter().map(|(id, weight)| (voter(id), *weight))).unwrap()
}

pub fn stable_quorum(weights: &[(&str, u64)]) -> MembershipQuorum {
    MembershipQuorum::Stable(membership(weights))
}

pub struct TempDb(PathBuf);

impl TempDb {
    pub fn new() -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "xen-rust-backend-{}-{id}.sqlite",
            std::process::id()
        ));
        Self(path)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
    }
}
