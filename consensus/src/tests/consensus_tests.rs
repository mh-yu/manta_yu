// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, HashMap, VecDeque};
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

// Fixture
fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn mock_committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
        sigma: 2,
        kappa: 2,
        reference: 3,
        coverage: 3,
    }
}

fn mock_certificate(
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<Digest>,
    solid_wave_vertices: BTreeSet<Digest>,
) -> (Digest, Certificate) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            solid_wave_vertices: solid_wave_vertices.iter().cloned().collect(),
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

fn make_round(
    round: Round,
    parents: &BTreeSet<Digest>,
    keys: &[PublicKey],
    solid_wave_vertices: &BTreeSet<Digest>,
) -> (VecDeque<Certificate>, BTreeSet<Digest>) {
    let mut certificates = VecDeque::new();
    let mut next_parents = BTreeSet::new();

    for name in keys {
        let (digest, certificate) = mock_certificate(
            *name,
            round,
            parents.clone(),
            solid_wave_vertices.clone(),
        );
        certificates.push_back(certificate);
        next_parents.insert(digest);
    }

    (certificates, next_parents)
}

#[tokio::test]
async fn commits_round_one_on_round_five_after_one_wave_warmup() {
    let committee = mock_committee();
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();
    let leader = keys[0];

    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    let (round_1, parents) = make_round(1, &genesis, &keys, &BTreeSet::new());
    let leader_round_1_digest = round_1
        .iter()
        .find(|certificate| certificate.origin() == leader)
        .unwrap()
        .digest();
    certificates.extend(round_1);

    let (round_2, parents) = make_round(2, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_2);

    let support = vec![leader_round_1_digest].into_iter().collect::<BTreeSet<_>>();
    let (round_3, parents) = make_round(3, &parents, &keys, &support);
    certificates.extend(round_3);

    let (round_4, parents) = make_round(4, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_4);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        committee,
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }
    assert!(timeout(Duration::from_millis(50), rx_output.recv()).await.is_err());

    let (_, trigger) = mock_certificate(keys[0], 5, parents, BTreeSet::new());
    tx_waiter.send(trigger).await.unwrap();

    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.round(), 1);
    assert_eq!(committed.origin(), leader);
}

#[tokio::test]
async fn does_not_commit_on_round_five_without_round_three_support() {
    let committee = mock_committee();
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();
    let leader = keys[0];

    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    let (round_1, parents) = make_round(1, &genesis, &keys, &BTreeSet::new());
    let leader_round_1_digest = round_1
        .iter()
        .find(|certificate| certificate.origin() == leader)
        .unwrap()
        .digest();
    certificates.extend(round_1);

    let (round_2, parents) = make_round(2, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_2);

    let (round_3, parents) = make_round(3, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_3);

    let support_round_1 = vec![leader_round_1_digest]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let (round_4, parents) = make_round(4, &parents, &keys, &support_round_1);
    certificates.extend(round_4);

    let (round_5, parents) = make_round(5, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_5);

    let (round_6, parents) = make_round(6, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_6);

    let (round_7, parents) = make_round(7, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_7);

    let (round_8, parents) = make_round(8, &parents, &keys, &BTreeSet::new());
    certificates.extend(round_8);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        committee,
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    assert!(timeout(Duration::from_millis(100), rx_output.recv()).await.is_err());

    let (_, trigger) = mock_certificate(keys[0], 5, parents, BTreeSet::new());
    tx_waiter.send(trigger).await.unwrap();

    assert!(timeout(Duration::from_millis(100), rx_output.recv()).await.is_err());
}
