// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SealedBatch;
use crate::worker::SignedBatchAck;
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use primary::PoaCertificate;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A sealed batch with a stable protocol digest.
    pub batch: SealedBatch,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for an availability quorum of signed acknowledgements.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<SealedBatch>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<SealedBatch>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(
        wait_for: CancelHandler,
        deliver: Stake,
        expected_author: PublicKey,
        expected_digest: Digest,
    ) -> Option<(Stake, PublicKey, crypto::Signature)> {
        let bytes = wait_for.await.ok()?;
        let ack: SignedBatchAck = bincode::deserialize(&bytes).ok()?;
        if ack.author != expected_author || ack.digest != expected_digest {
            return None;
        }
        ack.signature.verify(&ack.digest, &ack.author).ok()?;
        Some((deliver, ack.author, ack.signature))
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake, name, batch.digest.clone())
                })
                .collect();

            // Wait for the first f+1 signed acknowledgements. These signatures form the
            // POA carried by the primary/header path, while payload sync may continue
            // in the background on nodes that still miss the batch.
            let mut total_stake = 0;
            let mut acknowledgements = Vec::new();
            while let Some(Some((stake, author, signature))) = wait_for_quorum.next().await {
                total_stake += stake;
                acknowledgements.push((author, signature));
                if total_stake >= self.committee.validity_threshold() {
                    let poa = PoaCertificate {
                        digest: batch.digest.clone(),
                        acknowledgements,
                    };
                    let mut batch = batch;
                    batch.poa = Some(poa);
                    self.tx_batch
                        .send(batch)
                        .await
                        .expect("Failed to deliver batch");
                    break;
                }
            }
        }
    }
}
