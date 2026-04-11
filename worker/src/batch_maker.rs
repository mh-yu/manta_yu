// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
#[cfg(feature = "benchmark")]
use crypto::Hash as _;
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use primary::BatchPayload;
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the local `Processor`.
    tx_batch: Sender<SerializedBatchMessage>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_batch: Sender<SerializedBatchMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_batch,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal the current batch and deliver it to the local processor.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();

        #[cfg(feature = "benchmark")]
        {
            // Use the same digest as the coupled primary payload map so benchmark parsing
            // can join worker sample logs with primary Created/Committed logs.
            let digest = BatchPayload::new(0, batch.clone()).digest();

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        // In the coupled design payload is embedded in vertices, so workers no longer
        // proactively broadcast batches to peer workers before the primary proposes them.
        self.tx_batch
            .send(serialized)
            .await
            .expect("Failed to deliver batch");
    }
}
