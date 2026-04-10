// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use crate::messages::BatchPayload;
use crate::messages::ProposalParents;
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_batches, rx_our_batches) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Create a new test store.
    let path = ".db_test_propose_empty";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_batches,
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

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_batches, rx_our_batches) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Create a new test store.
    let path = ".db_test_propose_payload";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_batches,
        /* tx_core */ tx_headers,
        store.clone(),
    );

    // Round 1 is always bootstrapped and empty.
    let bootstrap = rx_headers.recv().await.unwrap();
    assert_eq!(bootstrap.round, 1);
    assert!(bootstrap.payload.is_empty());

    // Unlock round 2 so the proposer can materialize a payload-carrying header.
    tx_parents
        .send((ProposalParents::from(vec![bootstrap.digest()]), 1))
        .await
        .unwrap();

    // Send enough embedded payload for the next header payload.
    let payload = BatchPayload::new(0, vec![vec![name.0[0]; 32]]);
    let digest = payload.digest();
    tx_our_batches
        .send(payload.clone())
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 2);
    assert_eq!(header.payload.get(&digest), Some(&payload));
    assert!(header.verify(&committee()).is_ok());
}
