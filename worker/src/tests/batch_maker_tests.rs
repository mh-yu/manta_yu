// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{keys, transaction};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), 1, "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        tx_message,
        /* workers_addresses */ dummy_addresses,
        /* availability_threshold */ 1,
    );

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction(), transaction()];
    let QuorumWaiterMessage { batch, handlers } = rx_message.recv().await.unwrap();
    assert_eq!(handlers.len(), 1);
    match bincode::deserialize(&batch.serialized_batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), 1, "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        tx_message,
        /* workers_addresses */ dummy_addresses,
        /* availability_threshold */ 1,
    );

    // Do not send enough transactions to seal a batch..
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction()];
    let QuorumWaiterMessage { batch, handlers } = rx_message.recv().await.unwrap();
    assert_eq!(handlers.len(), 1);
    match bincode::deserialize(&batch.serialized_batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[test]
fn select_workers_for_broadcast_rotates_availability_subset() {
    let keys = keys();
    let workers = vec![
        (keys[0].0, 1, "127.0.0.1:1000".parse().unwrap()),
        (keys[1].0, 1, "127.0.0.1:1001".parse().unwrap()),
        (keys[2].0, 1, "127.0.0.1:1002".parse().unwrap()),
        (keys[3].0, 1, "127.0.0.1:1003".parse().unwrap()),
    ];

    let (selected, next_index) = BatchMaker::select_workers_for_broadcast(&workers, 2, 0);
    assert_eq!(
        selected.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        vec![keys[0].0, keys[1].0]
    );
    assert_eq!(next_index, 2);

    let (selected, next_index) =
        BatchMaker::select_workers_for_broadcast(&workers, 2, next_index);
    assert_eq!(
        selected.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        vec![keys[2].0, keys[3].0]
    );
    assert_eq!(next_index, 0);
}
