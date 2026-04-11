// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{BatchPayload, Certificate, Header};
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::debug;
use store::Store;
use tokio::sync::mpsc::Sender;

/// The `Synchronizer` checks if we have all batches and parents referenced by a header. If we don't, it sends
/// a command to the `Waiter` to request the missing data.
pub struct Synchronizer {
    /// Cached authority ordering to map to node ids in logs.
    authorities: Vec<PublicKey>,
    /// The persistent storage.
    store: Store,
    /// Send commands to the `HeaderWaiter`.
    tx_header_waiter: Sender<WaiterMessage>,
    /// Send commands to the `CertificateWaiter`.
    tx_certificate_waiter: Sender<Certificate>,
    /// The genesis and its digests.
    genesis: Vec<(Digest, Certificate)>,
}

impl Synchronizer {
    pub fn new(
        _name: PublicKey,
        committee: &Committee,
        store: Store,
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        let authorities = committee.authorities.keys().cloned().collect();
        Self {
            authorities,
            store,
            tx_header_waiter,
            tx_certificate_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
        }
    }

    fn payload_storage_key(author: &PublicKey, digest: &Digest) -> Vec<u8> {
        [author.as_ref(), digest.as_ref()].concat()
    }

    async fn persist_payload_if_missing(
        &mut self,
        author: &PublicKey,
        digest: &Digest,
        payload: &BatchPayload,
    ) -> DagResult<()> {
        let key = Self::payload_storage_key(author, digest);
        if self.store.read(key.clone()).await?.is_none() {
            let serialized =
                bincode::serialize(payload).expect("Failed to serialize embedded batch payload");
            self.store.write(key, serialized).await;
        }
        Ok(())
    }

    /// Returns `true` if the payload is missing. In the coupled design, payload travels inside
    /// the header itself, so we only need to validate and persist those embedded batches before
    /// voting.
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        for (digest, payload) in header.payload.iter() {
            self.persist_payload_if_missing(&header.author, digest, payload)
                .await?;
        }
        Ok(false)
    }

    /// Returns the parents of a header if we have them all. If at least one parent is missing,
    /// we return an empty vector and re-schedule processing of the header for when those
    /// parent certificates become available locally.
    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        for digest in &header.parents {
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == digest)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing.push(digest.clone()),
            };
        }

        if missing.is_empty() {
            return Ok(parents);
        }

        let mut missing_labels = Vec::with_capacity(missing.len());
        for digest in &missing {
            // Attempt to resolve if we already have it (best-effort).
            if let Some(bytes) = self.store.read(digest.to_vec()).await? {
                if let Ok(cert) = bincode::deserialize::<Certificate>(&bytes) {
                    let node_id = self
                        .authorities
                        .iter()
                        .position(|a| a == &cert.origin())
                        .unwrap_or(999);
                    let weak_prefix = if cert.round() + 1 < header.round {
                        "w"
                    } else {
                        ""
                    };
                    missing_labels.push(format!(
                        "{} [{}{},{}]",
                        digest,
                        weak_prefix,
                        cert.round(),
                        node_id
                    ));
                    continue;
                }
            }
            missing_labels.push(format!("{}", digest));
        }
        debug!(
            "Missing parent(s) for header {} (round {}): {}",
            header.id,
            header.round,
            missing_labels.join(", ")
        );
        self.tx_header_waiter
            .send(WaiterMessage::SyncParents(missing, header.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }

    /// Check whether we have all the ancestors of the certificate. If we don't, send the certificate to
    /// the local `CertificateWaiter`, which will trigger re-processing once the missing parent
    /// certificates are available in storage.
    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        for digest in &certificate.header.parents {
            if self.genesis.iter().any(|(x, _)| x == digest) {
                continue;
            }

            if self.store.read(digest.to_vec()).await?.is_none() {
                self.tx_certificate_waiter
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send sync certificate request");
                return Ok(false);
            };
        }
        Ok(true)
    }
}
