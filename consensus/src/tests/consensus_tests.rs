// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, HashSet, VecDeque};
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
        sigma: 1,
        kappa: 2,
        reference: 3,
        coverage: 3,
    }
}

fn mock_committee_with_params(sigma: usize, kappa: usize) -> Committee {
    let mut committee = mock_committee();
    committee.sigma = sigma;
    committee.kappa = kappa;
    committee
}

// Fixture
fn mock_certificate(
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<Digest>,
) -> (Digest, Certificate) {
    let parent_vertices: HashSet<_> = parents.iter().cloned().collect();
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            solid_step_vertices: parent_vertices.clone(),
            solid_step_vertices_merged: parent_vertices.clone(),
            solid_wave_vertices: parent_vertices.clone(),
            solid_wave_vertices_merged: parent_vertices,
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
fn make_certificates(
    start: Round,
    stop: Round,
    initial_parents: &BTreeSet<Digest>,
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<Digest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in start..=stop {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) = mock_certificate(*name, round, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

// Run for 4 dag rounds in ideal conditions (all nodes reference all other nodes). We should commit
// the leader of round 2.
#[tokio::test]
async fn commit_one() {
    // Make certificates for rounds 1 to 4.
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) = make_certificates(1, 4, &genesis, &keys);

    // Make one certificate with round 5 to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0], 5, next_parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(32);
    let (tx_output, mut rx_output) = channel(32);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the committed
    // leader); then the leader's certificate should be committed.
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 2);
}

#[test]
fn regular_commit_candidate_keeps_sigma_one_behavior() {
    let committee = mock_committee();
    let consensus = Consensus {
        committee: committee.clone(),
        authorities: committee.authorities.keys().copied().collect(),
        author_to_node: committee
            .authorities
            .keys()
            .copied()
            .enumerate()
            .map(|(index, authority)| (authority, index))
            .collect(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: channel(1).0,
        genesis: Certificate::genesis(&committee),
    };

    assert_eq!(consensus.regular_commit_candidate(4), None);
    assert_eq!(consensus.regular_commit_candidate(5), Some((2, 3)));
    assert_eq!(consensus.regular_commit_candidate(7), Some((4, 5)));
}

#[test]
fn regular_commit_candidate_special_cases_sigma_two() {
    let committee = mock_committee_with_params(2, 2);
    let consensus = Consensus {
        committee: committee.clone(),
        authorities: committee.authorities.keys().copied().collect(),
        author_to_node: committee
            .authorities
            .keys()
            .copied()
            .enumerate()
            .map(|(index, authority)| (authority, index))
            .collect(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: channel(1).0,
        genesis: Certificate::genesis(&committee),
    };

    assert_eq!(consensus.regular_commit_candidate(6), None);
    assert_eq!(consensus.regular_commit_candidate(8), Some((4, 6)));
    assert_eq!(consensus.regular_commit_candidate(12), Some((8, 10)));
}

#[test]
fn early_commit_gate_requires_leader_support_stake() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let committee = mock_committee();
    let consensus = Consensus {
        committee: committee.clone(),
        authorities: committee.authorities.keys().copied().collect(),
        author_to_node: committee
            .authorities
            .keys()
            .copied()
            .enumerate()
            .map(|(index, authority)| (authority, index))
            .collect(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: channel(1).0,
        genesis: Certificate::genesis(&committee),
    };

    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let mut state = State::new(Certificate::genesis(&committee));

    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (round_one, parents) = make_certificates(1, 1, &genesis, &nodes);
    for certificate in round_one {
        state.insert(certificate);
    }

    let (leader_2_digest, leader_2_certificate) = mock_certificate(keys[0], 2, parents.clone());
    state.insert(leader_2_certificate);
    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (round_two, mut parents) = make_certificates(2, 2, &parents, &nodes);
    for certificate in round_two {
        state.insert(certificate);
    }

    let mut next_parents = BTreeSet::new();

    let (digest, certificate) = mock_certificate(keys[1], 3, parents.clone());
    state.insert(certificate);
    next_parents.insert(digest);

    let (digest, certificate) = mock_certificate(keys[2], 3, parents.clone());
    state.insert(certificate);
    next_parents.insert(digest);

    parents.insert(leader_2_digest);
    let (digest, certificate) = mock_certificate(keys[0], 3, parents);
    state.insert(certificate);
    next_parents.insert(digest);

    assert!(state.dag.get(&3).unwrap().len() >= committee.validity_threshold() as usize);
    assert!(!consensus.early_commit_threshold_reached(&state, 2, 3));
}

#[tokio::test]
async fn commit_on_support_round_without_waiting_for_trigger_round() {
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) = make_certificates(1, 2, &genesis, &keys);
    let (mut support_round_certificates, _) = make_certificates(3, 3, &next_parents, &keys);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(32);
    let (tx_output, mut rx_output) = channel(8);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    tx_waiter
        .send(support_round_certificates.pop_front().unwrap())
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(100), rx_output.recv()).await.is_err());
    tx_waiter
        .send(support_round_certificates.pop_front().unwrap())
        .await
        .unwrap();

    for _ in 1..=4 {
        let certificate = timeout(Duration::from_secs(1), rx_output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(certificate.round(), 1);
    }
    let certificate = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.round(), 2);
}

// Run for 8 dag rounds with one dead node node (that is not a leader). We should commit the leaders of
// rounds 2, 4, and 6.
#[tokio::test]
async fn dead_node() {
    // Make the certificates.
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_certificates(1, 9, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(32);
    let (tx_output, mut rx_output) = channel(32);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    // We should commit 3 leaders (rounds 2, 4, and 6).
    for i in 1..=15 {
        let certificate = rx_output.recv().await.unwrap();
        let expected = ((i - 1) / keys.len() as u64) + 1;
        assert_eq!(certificate.round(), expected);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 6);
}

// Run for 6 dag rounds. The leaders of round 2 does not have enough support, but the leader of
// round 4 does. The leader of rounds 2 and 4 should thus be committed upon entering round 6.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Round 1: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (out, parents) = make_certificates(1, 1, &genesis, &nodes);
    certificates.extend(out);

    // Round 2: Fully connect graph. But remember the digest of the leader. Note that this
    // round is the only one with 4 certificates.
    let (leader_2_digest, certificate) = mock_certificate(keys[0], 2, parents.clone());
    certificates.push_back(certificate);

    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, mut parents) = make_certificates(2, 2, &parents, &nodes);
    certificates.extend(out);

    // Round 3: Only node 0 links to the leader of round 2.
    let mut next_parents = BTreeSet::new();

    let name = &keys[1];
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[2];
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[0];
    parents.insert(leader_2_digest);
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    parents = next_parents.clone();

    // Rounds 4, 5, and 6: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (out, parents) = make_certificates(4, 6, &parents, &nodes);
    certificates.extend(out);

    // Round 7: Send a single certificate to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0], 7, parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(32);
    let (tx_output, mut rx_output) = channel(32);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // We should commit 2 leaders (rounds 2 and 4).
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 2);
    }
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 3);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 4);
}

// Run for 6 dag rounds. Node 0 (the leader of round 2) is missing for rounds 1 and 2,
// and reapers from round 3.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Remove the leader for rounds 1 and 2.
    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, parents) = make_certificates(1, 2, &genesis, &nodes);
    certificates.extend(out);

    // Add back the leader for rounds 3, 4, 5 and 6.
    let (out, parents) = make_certificates(3, 6, &parents, &keys);
    certificates.extend(out);

    // Add a certificate of round 7 to commit the leader of round 4.
    let (_, certificate) = mock_certificate(keys[0], 7, parents.clone());
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(32);
    let (tx_output, mut rx_output) = channel(32);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. We should only commit upon receiving the last
    // certificate, so calls below should not block the task.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // Ensure the commit sequence is as expected.
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 2);
    }
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 3);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 4);
}
