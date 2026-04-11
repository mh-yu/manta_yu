// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{certificate, committee, committee_with_base_port, header, keys, listener, votes};
use crypto::Signature;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn process_header() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, secret) = keys.pop().unwrap();
    let mut signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the vote we expect to receive.
    let expected = Vote::new(&header(), &name, &mut signature_service).await;

    // Spawn a listener to receive the vote.
    let address = committee
        .primary(&header().author)
        .unwrap()
        .primary_to_primary;
    let handle = listener(address);

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* adaptive_wait_enabled */ true,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote.
    let received: bytes::Bytes = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    // Ensure the header is correctly stored.
    let stored = store
        .read(header().id.to_vec())
        .await
        .unwrap()
        .map(|x| bincode::deserialize(&x).unwrap());
    assert_eq!(stored, Some(header()));
}

#[tokio::test]
async fn process_header_missing_parent() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_parent";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* adaptive_wait_enabled */ true,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        parents: [Digest::default()].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_missing_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* adaptive_wait_enabled */ true,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        payload: [(Digest::default(), 0)].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_votes() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let remote_header = header();

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* adaptive_wait_enabled */ true,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    tx_primary_messages
        .send(PrimaryMessage::Header(remote_header.clone()))
        .await
        .unwrap();

    // Send votes to the core and ensure they locally form a certificate.
    for vote in votes(&remote_header) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    let delivered = tokio::time::timeout(
        tokio::time::Duration::from_millis(300),
        rx_consensus.recv(),
    )
    .await
    .expect("locally formed certificate was not delivered to consensus in time")
    .unwrap();
    assert_eq!(delivered.header.id, remote_header.id);
}

#[tokio::test]
async fn locally_formed_certificates_unlock_parents() {
    let (name, secret) = keys().into_iter().nth(2).unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_200);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(8);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(3);
    let (tx_parents, mut rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_locally_formed_certificates_unlock_parents";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* adaptive_wait_enabled */ true,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    let remote_headers: Vec<_> = keys()
        .into_iter()
        .filter(|(author, _)| *author != name)
        .take(3)
        .map(|(author, secret)| {
            let header = Header {
                author,
                round: 1,
                parents: Certificate::genesis(&committee)
                    .iter()
                    .map(|x| x.digest())
                    .collect(),
                ..Header::default()
            };
            Header {
                id: header.digest(),
                signature: Signature::new(&header.digest(), &secret),
                ..header
            }
        })
        .collect();

    for header in remote_headers.iter() {
        tx_primary_messages
            .send(PrimaryMessage::Header(header.clone()))
            .await
            .unwrap();
    }

    for header in remote_headers.iter() {
        for vote in votes(header) {
            tx_primary_messages
                .send(PrimaryMessage::Vote(vote))
                .await
                .unwrap();
        }
    }

    // Ensure the core sends the parents of the certificates to the proposer.
    let received = rx_parents.recv().await.unwrap();
    let certificates: Vec<_> = remote_headers.iter().map(certificate).collect();
    let expected_parent_set: HashSet<_> = certificates.iter().map(|x| x.digest()).collect();
    let received_parent_set: HashSet<_> = received.0.parents.into_iter().collect();
    assert_eq!(received.1, 1);
    assert_eq!(received_parent_set, expected_parent_set);

    // Ensure the core sends the certificates to the consensus.
    for x in certificates.clone() {
        let received = rx_consensus.recv().await.unwrap();
        assert_eq!(received, x);
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = store.read(x.digest().to_vec()).await.unwrap().unwrap();
        let stored: Certificate = bincode::deserialize(&stored).unwrap();
        assert_eq!(stored.header.id, x.header.id);
        assert_eq!(stored.round(), x.round());
        assert_eq!(stored.origin(), x.origin());
        assert!(stored.verify(&committee).is_ok());
    }
}

#[tokio::test]
async fn adaptive_wait_absorbs_late_certificate() {
    let (name, secret) = keys().into_iter().nth(2).unwrap();
    let (_, local_header_secret) = keys().into_iter().nth(2).unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_250);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(4);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(4);
    let (tx_parents, mut rx_parents) = channel(2);

    let path = ".db_test_adaptive_wait_absorbs_late_certificate";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee.clone(),
        store,
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        true,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let mut remote_headers: Vec<_> = keys()
        .into_iter()
        .filter(|(author, _)| *author != name)
        .take(3)
        .map(|(author, secret)| {
            let header = Header {
                author,
                round: 1,
                parents: Certificate::genesis(&committee)
                    .iter()
                    .map(|x| x.digest())
                    .collect(),
                ..Header::default()
            };
            Header {
                id: header.digest(),
                signature: Signature::new(&header.digest(), &secret),
                ..header
            }
        })
        .collect();
    let local_header = {
        let header = Header {
            author: name,
            round: 1,
            parents: Certificate::genesis(&committee)
                .iter()
                .map(|x| x.digest())
                .collect(),
            ..Header::default()
        };
        Header {
            id: header.digest(),
            signature: Signature::new(&header.digest(), &local_header_secret),
            ..header
        }
    };
    remote_headers.push(local_header);

    tx_primary_messages
        .send(PrimaryMessage::Header(remote_headers[3].clone()))
        .await
        .unwrap();
    let late_votes: Vec<_> = votes(&remote_headers[3])
        .into_iter()
        .filter(|vote| vote.author != name)
        .collect();
    for vote in late_votes.iter().take(1).cloned() {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    for header in remote_headers.iter().take(3) {
        tx_primary_messages
            .send(PrimaryMessage::Header(header.clone()))
            .await
            .unwrap();
        for vote in votes(header) {
            tx_primary_messages
                .send(PrimaryMessage::Vote(vote))
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    for vote in late_votes.into_iter().skip(1) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    let received = tokio::time::timeout(
        tokio::time::Duration::from_millis(200),
        rx_parents.recv(),
    )
    .await
    .expect("adaptive wait did not release in time")
    .unwrap();

    let received_parents: HashSet<_> = received.0.parents.into_iter().collect();
    let expected_certificates: Vec<_> = remote_headers.iter().map(certificate).collect();
    let expected_parents: HashSet<_> = expected_certificates.iter().map(|x| x.digest()).collect();
    assert_eq!(received.1, 1);
    assert_eq!(received_parents, expected_parents);
}

#[tokio::test]
async fn adaptive_wait_can_be_disabled() {
    let (name, secret) = keys().into_iter().nth(2).unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_275);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(4);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(4);
    let (tx_parents, mut rx_parents) = channel(2);

    let path = ".db_test_adaptive_wait_can_be_disabled";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee.clone(),
        store,
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        false,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let remote_headers: Vec<_> = keys()
        .into_iter()
        .filter(|(author, _)| *author != name)
        .take(4)
        .map(|(author, secret)| {
            let header = Header {
                author,
                round: 1,
                parents: Certificate::genesis(&committee)
                    .iter()
                    .map(|x| x.digest())
                    .collect(),
                ..Header::default()
            };
            Header {
                id: header.digest(),
                signature: Signature::new(&header.digest(), &secret),
                ..header
            }
        })
        .collect();

    for header in remote_headers.iter().take(3) {
        tx_primary_messages
            .send(PrimaryMessage::Header(header.clone()))
            .await
            .unwrap();
        for vote in votes(header) {
            tx_primary_messages
                .send(PrimaryMessage::Vote(vote))
                .await
                .unwrap();
        }
    }

    let received = tokio::time::timeout(
        tokio::time::Duration::from_millis(200),
        rx_parents.recv(),
    )
    .await
    .expect("disabled adaptive wait did not release immediately")
    .unwrap();

    let received_parents: HashSet<_> = received.0.parents.into_iter().collect();
    let expected_certificates: Vec<_> = remote_headers.iter().map(certificate).collect();
    let expected_parents: HashSet<_> =
        expected_certificates[..3].iter().map(|x| x.digest()).collect();
    assert_eq!(received.1, 1);
    assert_eq!(received_parents, expected_parents);
}

#[tokio::test]
async fn process_votes_for_known_remote_header() {
    let mut all_keys = keys();
    let (header_author, header_secret) = all_keys.pop().unwrap();
    let (name, secret) = all_keys.pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_300);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(8);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(8);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_process_votes_for_known_remote_header";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee.clone(),
        store,
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        true,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let remote_header = {
        let header = Header {
            author: header_author,
            round: 1,
            parents: Certificate::genesis(&committee)
                .iter()
                .map(|x| x.digest())
                .collect(),
            ..Header::default()
        };
        Header {
            id: header.digest(),
            signature: Signature::new(&header.digest(), &header_secret),
            ..header
        }
    };

    tx_primary_messages
        .send(PrimaryMessage::Header(remote_header.clone()))
        .await
        .unwrap();

    for vote in votes(&remote_header) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    let delivered = tokio::time::timeout(
        tokio::time::Duration::from_millis(300),
        rx_consensus.recv(),
    )
    .await
    .expect("remote header did not get certified in time")
    .unwrap();
    assert_eq!(delivered.header.id, remote_header.id);
}
