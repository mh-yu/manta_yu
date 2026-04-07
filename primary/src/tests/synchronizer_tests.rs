use super::*;
use crate::common::{committee, keys};
use crate::header_waiter::WaiterMessage;
use crate::messages::{Certificate, Header};
use crate::primary::PoaCertificate;
use crypto::{Digest, Signature};
use std::fs;
use tokio::sync::mpsc::channel;

fn header_with_payload_and_poa(include_poa: bool) -> Header {
    let committee = committee();
    let (author, secret) = keys().pop().unwrap();
    let digest = Digest::default();

    let payload = [(digest.clone(), 0)].iter().cloned().collect();
    let payload_poas = if include_poa {
        let acknowledgements = keys()
            .into_iter()
            .take(committee.validity_threshold() as usize)
            .map(|(name, secret)| (name, Signature::new(&digest, &secret)))
            .collect();
        [(digest.clone(), PoaCertificate { digest: digest.clone(), acknowledgements })]
            .iter()
            .cloned()
            .collect()
    } else {
        Default::default()
    };

    let header = Header {
        author,
        round: 1,
        payload,
        payload_poas,
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
}

#[tokio::test]
async fn missing_payload_blocks_without_poa() {
    let mut all_keys = keys();
    let _ = all_keys.pop().unwrap();
    let (name, _) = all_keys.pop().unwrap();
    let (tx_header_waiter, mut rx_header_waiter) = channel(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = channel(1);

    let path = ".db_test_missing_payload_blocks_without_poa";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let mut synchronizer = Synchronizer::new(
        name,
        &committee(),
        store,
        tx_header_waiter,
        tx_certificate_waiter,
    );

    let header = header_with_payload_and_poa(false);
    assert!(header.verify(&committee()).is_ok());
    assert!(synchronizer.missing_payload(&header).await.unwrap());

    match rx_header_waiter.recv().await.unwrap() {
        WaiterMessage::SyncBatches {
            missing,
            header: received_header,
            wait,
        } => {
            assert!(wait);
            assert_eq!(received_header.id, header.id);
            assert_eq!(missing.len(), 1);
        }
        message => panic!("Unexpected waiter message: {:?}", message),
    }
}

#[tokio::test]
async fn missing_payload_continues_with_poa() {
    let mut all_keys = keys();
    let _ = all_keys.pop().unwrap();
    let (name, _) = all_keys.pop().unwrap();
    let (tx_header_waiter, mut rx_header_waiter) = channel(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = channel(1);

    let path = ".db_test_missing_payload_continues_with_poa";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let mut synchronizer = Synchronizer::new(
        name,
        &committee(),
        store,
        tx_header_waiter,
        tx_certificate_waiter,
    );

    let header = header_with_payload_and_poa(true);
    assert!(header.verify(&committee()).is_ok());
    assert!(!synchronizer.missing_payload(&header).await.unwrap());

    match rx_header_waiter.recv().await.unwrap() {
        WaiterMessage::SyncBatches {
            missing,
            header: received_header,
            wait,
        } => {
            assert!(!wait);
            assert_eq!(received_header.id, header.id);
            assert_eq!(missing.len(), 1);
        }
        message => panic!("Unexpected waiter message: {:?}", message),
    }
}
