// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crate::primary::Round;
use crypto::Digest;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The commands that can be sent to the `Waiter`.
#[derive(Debug)]
pub enum WaiterMessage {
    SyncParents(Vec<Digest>, Header),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,
    /// List of digests (either certificates, headers or tx batch) that are waiting
    /// to be processed. Their processing will resume when we get all their dependencies.
    pending: std::collections::HashMap<Digest, (Round, Sender<()>)>,
}

impl HeaderWaiter {
    pub fn spawn(
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                consensus_round,
                gc_depth,
                rx_synchronizer,
                tx_core,
                pending: std::collections::HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncParents(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to us
                            // when all its parents are in the store.
                            let wait_for = missing
                                .iter()
                                .cloned()
                                .map(|x| (x.to_vec(), self.store.clone()))
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        debug!(
                            "All batches received for header {} (round {}), sending back to Core for reprocessing",
                            header.id,
                            header.round
                        );
                        let _ = self.pending.remove(&header.id);
                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {
                        debug!("Header waiter request was canceled (likely due to GC)");
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                let mut canceled_count = 0;
                for (header_id, (r, handler)) in &self.pending {
                    if r <= &gc_round {
                        debug!(
                            "Canceling header waiter for header {} (round {}) due to GC (gc_round={}, consensus_round={})",
                            header_id, r, gc_round, round
                        );
                        let _ = handler.send(()).await;
                        canceled_count += 1;
                    }
                }
                if canceled_count > 0 {
                    debug!(
                        "GC cleanup: canceled {} pending header waiter(s) for rounds <= {}",
                        canceled_count, gc_round
                    );
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
            }
        }
    }
}
