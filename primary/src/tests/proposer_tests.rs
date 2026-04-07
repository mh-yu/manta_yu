// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let path = ".db_test_propose_empty";
    let _ = fs::remove_dir_all(path);
    let store = store::Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
        store.clone(),
    );

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    let committee = committee();
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let genesis_parent = Certificate::genesis(&committee)
        .iter()
        .next()
        .unwrap()
        .digest();
    let (_tx_parents, rx_parents) = channel::<(ProposalParents, Round)>(1);
    let (_tx_our_digests, rx_our_digests) = channel::<(Digest, WorkerId)>(1);
    let (tx_headers, _rx_headers) = channel::<Header>(1);

    let mut unlocked_rounds = HashMap::new();
    unlocked_rounds.insert(
        2,
        UnlockedRound {
            parents: vec![genesis_parent.clone()],
            solid_step_union: HashSet::new(),
            solid_wave_union: HashSet::new(),
            ready_since: Instant::now(),
            unlock_order: 0,
        },
    );
    unlocked_rounds.insert(
        3,
        UnlockedRound {
            parents: vec![genesis_parent],
            solid_step_union: HashSet::new(),
            solid_wave_union: HashSet::new(),
            ready_since: Instant::now(),
            unlock_order: 1,
        },
    );

    let digest = Digest(name.0);
    let proposer = Proposer {
        name,
        node_id: None,
        signature_service,
        header_size: 32,
        max_header_delay: 1_000,
        rx_core: rx_parents,
        rx_workers: rx_our_digests,
        tx_core: tx_headers,
        unlocked_rounds,
        proposed_rounds: HashSet::new(),
        next_unlock_order: 2,
        digests: VecDeque::from(vec![(digest, 0)]),
        payload_size: 32,
        solid_step_length: committee.solid_step_length(),
        solid_wave_length: committee.solid_wave_length(),
        parent_grace_delay: Duration::from_millis(0),
    };

    let decision = proposer.next_proposal_round(true, true).unwrap();
    assert_eq!(decision.round, 3);
    assert!(decision.include_payload);
}

#[tokio::test]
async fn intermediate_round_does_not_take_payload() {
    let committee = committee();
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let genesis_parent = Certificate::genesis(&committee)
        .iter()
        .next()
        .unwrap()
        .digest();
    let (tx_parents, rx_parents) = channel::<(ProposalParents, Round)>(1);
    let (tx_our_digests, rx_our_digests) = channel::<(Digest, WorkerId)>(1);
    let (tx_headers, _rx_headers) = channel::<Header>(1);

    drop(tx_parents);
    drop(tx_our_digests);

    let mut unlocked_rounds = HashMap::new();
    unlocked_rounds.insert(
        2,
        UnlockedRound {
            parents: vec![genesis_parent],
            solid_step_union: HashSet::new(),
            solid_wave_union: HashSet::new(),
            ready_since: Instant::now(),
            unlock_order: 0,
        },
    );

    let proposer = Proposer {
        name,
        node_id: None,
        signature_service,
        header_size: 32,
        max_header_delay: 1_000,
        rx_core: rx_parents,
        rx_workers: rx_our_digests,
        tx_core: tx_headers,
        unlocked_rounds,
        proposed_rounds: HashSet::new(),
        next_unlock_order: 1,
        digests: VecDeque::from(vec![(Digest(name.0), 0)]),
        payload_size: 32,
        solid_step_length: committee.solid_step_length(),
        solid_wave_length: committee.solid_wave_length(),
        parent_grace_delay: Duration::from_millis(0),
    };

    let decision = proposer.next_proposal_round(true, true).unwrap();
    assert_eq!(decision.round, 2);
    assert!(!decision.include_payload);
}

#[tokio::test]
async fn intermediate_round_takes_only_overflow_payload() {
    let committee = committee();
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let genesis_parent = Certificate::genesis(&committee)
        .iter()
        .next()
        .unwrap()
        .digest();
    let (tx_parents, rx_parents) = channel::<(ProposalParents, Round)>(1);
    let (tx_our_digests, rx_our_digests) = channel::<(Digest, WorkerId)>(1);
    let (tx_headers, _rx_headers) = channel::<Header>(1);

    drop(tx_parents);
    drop(tx_our_digests);

    let mut unlocked_rounds = HashMap::new();
    unlocked_rounds.insert(
        2,
        UnlockedRound {
            parents: vec![genesis_parent],
            solid_step_union: HashSet::new(),
            solid_wave_union: HashSet::new(),
            ready_since: Instant::now(),
            unlock_order: 0,
        },
    );

    let digests = vec![
        (Digest([1; 32]), 0),
        (Digest([2; 32]), 0),
        (Digest([3; 32]), 0),
    ];
    let mut proposer = Proposer {
        name,
        node_id: None,
        signature_service,
        header_size: 32,
        max_header_delay: 1_000,
        rx_core: rx_parents,
        rx_workers: rx_our_digests,
        tx_core: tx_headers,
        unlocked_rounds,
        proposed_rounds: HashSet::new(),
        next_unlock_order: 1,
        digests: VecDeque::from(digests.clone()),
        payload_size: 96,
        solid_step_length: committee.solid_step_length(),
        solid_wave_length: committee.solid_wave_length(),
        parent_grace_delay: Duration::from_millis(0),
    };

    let decision = proposer.next_proposal_round(true, true).unwrap();
    assert_eq!(decision.round, 2);
    assert!(decision.include_payload);

    let payload = proposer.take_payload_for_header(32);
    assert_eq!(payload.len(), 2);
    assert!(payload.contains_key(&digests[0].0));
    assert!(payload.contains_key(&digests[1].0));
    assert_eq!(proposer.payload_size, 32);
    assert_eq!(proposer.digests.len(), 1);
    assert_eq!(proposer.digests.front(), Some(&digests[2]));
}
