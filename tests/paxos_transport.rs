mod common;

use common::voter;
use std::collections::BTreeSet;
use xen_rust_backend::cluster::paxos::{
    AcceptRequest, AcceptedResponse, AcceptedValue, Ballot, ChosenValue, PrepareRequest,
    PromiseResponse,
};
use xen_rust_backend::cluster::transport::{PaxosRpc, RpcCodecError};

fn ballot(round: u64, proposer: &str) -> Ballot {
    Ballot::new(round, voter(proposer))
}

fn round_trip(message: PaxosRpc) {
    let encoded = message.encode().unwrap();
    assert_eq!(PaxosRpc::decode(&encoded).unwrap(), message);
}

#[test]
fn all_paxos_rpc_variants_round_trip_without_losing_fields() {
    let accepted = AcceptedValue {
        ballot: ballot(u64::MAX, "node-c"),
        value: vec![0, 1, 0xff],
    };
    let messages = [
        PaxosRpc::Prepare(PrepareRequest {
            slot: u64::MAX,
            ballot: ballot(1, "node-a"),
        }),
        PaxosRpc::Promise(PromiseResponse::Promised {
            slot: 2,
            ballot: ballot(4, "node-b"),
            voter: voter("node-a"),
            accepted: None,
        }),
        PaxosRpc::Promise(PromiseResponse::Promised {
            slot: 3,
            ballot: ballot(5, "node-b"),
            voter: voter("node-c"),
            accepted: Some(accepted),
        }),
        PaxosRpc::Promise(PromiseResponse::Rejected {
            slot: 4,
            ballot: ballot(6, "node-a"),
            voter: voter("node-b"),
            promised: ballot(7, "node-c"),
        }),
        PaxosRpc::Accept(AcceptRequest {
            slot: 5,
            ballot: ballot(8, "node-a"),
            value: Vec::new(),
        }),
        PaxosRpc::Accepted(AcceptedResponse::Accepted {
            slot: 6,
            ballot: ballot(9, "node-b"),
            voter: voter("node-c"),
        }),
        PaxosRpc::Accepted(AcceptedResponse::Rejected {
            slot: 7,
            ballot: ballot(10, "node-c"),
            voter: voter("node-a"),
            promised: ballot(11, "node-b"),
        }),
        PaxosRpc::Learn(ChosenValue {
            slot: 8,
            ballot: ballot(12, "node-a"),
            value: b"chosen value".to_vec(),
            acceptors: [voter("node-a"), voter("node-b")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        }),
        PaxosRpc::Learned {
            slot: 8,
            voter: voter("node-c"),
        },
    ];

    for message in messages {
        round_trip(message);
    }
}

#[test]
fn decoder_rejects_bad_header_version_kind_and_trailing_data() {
    assert!(matches!(
        PaxosRpc::decode(b""),
        Err(RpcCodecError::Truncated)
    ));

    let mut bytes = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, "a"),
    })
    .encode()
    .unwrap();
    bytes[0] = b'Y';
    assert_eq!(PaxosRpc::decode(&bytes), Err(RpcCodecError::InvalidMagic));

    let mut bytes = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, "a"),
    })
    .encode()
    .unwrap();
    bytes[4] = 2;
    assert_eq!(
        PaxosRpc::decode(&bytes),
        Err(RpcCodecError::UnsupportedVersion(2))
    );

    let mut bytes = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, "a"),
    })
    .encode()
    .unwrap();
    bytes[5] = 99;
    assert_eq!(
        PaxosRpc::decode(&bytes),
        Err(RpcCodecError::UnknownMessageKind(99))
    );

    let mut bytes = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, "a"),
    })
    .encode()
    .unwrap();
    bytes.push(0);
    assert_eq!(PaxosRpc::decode(&bytes), Err(RpcCodecError::TrailingBytes));
}

#[test]
fn decoder_rejects_truncated_fields_and_invalid_response_tags() {
    let bytes = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, "a"),
    })
    .encode()
    .unwrap();
    assert!(matches!(
        PaxosRpc::decode(&bytes[..bytes.len() - 1]),
        Err(RpcCodecError::Truncated)
    ));

    let mut bytes = PaxosRpc::Promise(PromiseResponse::Promised {
        slot: 1,
        ballot: ballot(1, "a"),
        voter: voter("a"),
        accepted: None,
    })
    .encode()
    .unwrap();
    bytes[6] = 42;
    assert_eq!(PaxosRpc::decode(&bytes), Err(RpcCodecError::InvalidTag(42)));

    let mut bytes = PaxosRpc::Accepted(AcceptedResponse::Accepted {
        slot: 1,
        ballot: ballot(1, "a"),
        voter: voter("a"),
    })
    .encode()
    .unwrap();
    bytes[6] = 42;
    assert_eq!(PaxosRpc::decode(&bytes), Err(RpcCodecError::InvalidTag(42)));
}

#[test]
fn decoder_rejects_invalid_utf8_and_blank_voter_ids() {
    let mut invalid_utf8 = b"XPAX\x01\x00".to_vec();
    invalid_utf8.extend_from_slice(&1_u64.to_be_bytes());
    invalid_utf8.extend_from_slice(&1_u64.to_be_bytes());
    invalid_utf8.extend_from_slice(&1_u32.to_be_bytes());
    invalid_utf8.push(0xff);
    assert_eq!(
        PaxosRpc::decode(&invalid_utf8),
        Err(RpcCodecError::InvalidUtf8)
    );

    let mut blank_id = b"XPAX\x01\x00".to_vec();
    blank_id.extend_from_slice(&1_u64.to_be_bytes());
    blank_id.extend_from_slice(&1_u64.to_be_bytes());
    blank_id.extend_from_slice(&1_u32.to_be_bytes());
    blank_id.push(b' ');
    assert_eq!(
        PaxosRpc::decode(&blank_id),
        Err(RpcCodecError::InvalidVoterId)
    );
}

#[test]
fn voter_id_byte_limit_is_enforced_on_encode_and_decode() {
    let boundary_id = "x".repeat(1024);
    let valid_boundary = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, &boundary_id),
    });
    round_trip(valid_boundary);

    let long_id = "x".repeat(1025);
    let message = PaxosRpc::Prepare(PrepareRequest {
        slot: 1,
        ballot: ballot(1, &long_id),
    });
    assert_eq!(message.encode(), Err(RpcCodecError::VoterIdTooLong));

    let mut encoded = b"XPAX\x01\x00".to_vec();
    encoded.extend_from_slice(&1_u64.to_be_bytes());
    encoded.extend_from_slice(&1_u64.to_be_bytes());
    encoded.extend_from_slice(&1025_u32.to_be_bytes());
    encoded.extend(std::iter::repeat(b'x').take(1025));
    assert_eq!(
        PaxosRpc::decode(&encoded),
        Err(RpcCodecError::VoterIdTooLong)
    );
}

#[test]
fn chosen_certificates_reject_duplicate_or_excessive_acceptor_lists() {
    let mut duplicate_ids = b"XPAX\x01\x04".to_vec();
    duplicate_ids.extend_from_slice(&1_u64.to_be_bytes());
    duplicate_ids.extend_from_slice(&1_u64.to_be_bytes());
    duplicate_ids.extend_from_slice(&1_u32.to_be_bytes()); // proposer ID length
    duplicate_ids.push(b'a');
    duplicate_ids.extend_from_slice(&0_u32.to_be_bytes()); // empty chosen value
    duplicate_ids.extend_from_slice(&2_u32.to_be_bytes());
    for _ in 0..2 {
        duplicate_ids.extend_from_slice(&1_u32.to_be_bytes());
        duplicate_ids.push(b'a');
    }
    assert_eq!(
        PaxosRpc::decode(&duplicate_ids),
        Err(RpcCodecError::DuplicateAcceptor(voter("a")))
    );

    let mut too_many = b"XPAX\x01\x04".to_vec();
    too_many.extend_from_slice(&1_u64.to_be_bytes());
    too_many.extend_from_slice(&1_u64.to_be_bytes());
    too_many.extend_from_slice(&1_u32.to_be_bytes());
    too_many.push(b'a');
    too_many.extend_from_slice(&0_u32.to_be_bytes());
    too_many.extend_from_slice(&4097_u32.to_be_bytes());
    assert_eq!(
        PaxosRpc::decode(&too_many),
        Err(RpcCodecError::TooManyItems)
    );

    let boundary = ChosenValue {
        slot: 1,
        ballot: ballot(1, "a"),
        value: Vec::new(),
        acceptors: (0..4096)
            .map(|index| voter(&format!("voter-{index}")))
            .collect(),
    };
    round_trip(PaxosRpc::Learn(boundary));
}

#[test]
fn encoder_and_decoder_enforce_message_size_limits() {
    let oversized = vec![0; 16 * 1024 * 1024 + 1];
    let message = PaxosRpc::Accept(AcceptRequest {
        slot: 1,
        ballot: ballot(1, "a"),
        value: oversized.clone(),
    });
    assert_eq!(message.encode(), Err(RpcCodecError::MessageTooLarge));
    assert_eq!(
        PaxosRpc::decode(&oversized),
        Err(RpcCodecError::MessageTooLarge)
    );
}
