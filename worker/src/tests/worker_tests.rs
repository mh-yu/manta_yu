// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    batch_digest, committee_with_base_port, keys, signed_batch_listener, transaction,
    worker_primary_listener,
};
use network::SimpleSender;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, secret) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, secret, id, committee.clone(), parameters, store);

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let handle = worker_primary_listener(
        primary_address,
        batch_digest(),
        id,
        committee.validity_threshold() as usize,
    );

    // Spawn enough workers' listeners to acknowledge our batches.
    for (worker_name, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let (_, secret) = keys()
            .into_iter()
            .find(|(public_key, _)| public_key == &worker_name)
            .unwrap();
        let _ = signed_batch_listener(address, /* expected */ None, worker_name, secret);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}
