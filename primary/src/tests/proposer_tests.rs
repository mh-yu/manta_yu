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
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Create a new test store.
    let path = ".db_test_propose_payload";
    let _ = fs::remove_dir_all(path);
    let store = store::Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
        store.clone(),
    );

    // Round 1 is always proposed first without payload.
    let bootstrap = rx_headers.recv().await.unwrap();
    assert_eq!(bootstrap.round, 1);
    assert!(bootstrap.payload.is_empty());

    let genesis_parents = ProposalParents::from(
        Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect::<Vec<_>>(),
    );
    tx_parents.send((genesis_parents, 1)).await.unwrap();
    tx_parents
        .send((ProposalParents::from(vec![digest]), 2))
        .await
        .unwrap();

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer waits for the critical round and places the payload there.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 3);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
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
