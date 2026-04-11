// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crate::primary::{PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

/// The resolution of the timer that checks whether we received replies to our sync requests, and triggers
/// new sync requests if we didn't.
const TIMER_RESOLUTION: u64 = 1_000;

/// The commands that can be sent to the `Waiter`.
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header),
    SyncParents(Vec<Digest>, Header),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
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
    /// Network driver allowing to send batch sync messages to workers.
    network: SimpleSender,

    /// Keeps the digests of the all tx batches for which we sent a sync request,
    /// similarly to `header_requests`.
    batch_requests: HashMap<Digest, Round>,
    /// List of digests (either certificates, headers or tx batch) that are waiting
    /// to be processed. Their processing will resume when we get all their dependencies.
    pending: HashMap<Digest, (Round, Sender<()>)>,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        _sync_retry_delay: u64,
        _sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                consensus_round,
                gc_depth,
                rx_synchronizer,
                tx_core,
                network: SimpleSender::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
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

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches(missing, header) => {
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;
                            let missing_count = missing.len();
                            debug!(
                                "Synching the payload of header {} (round {}): missing {} batch(es)",
                                header_id,
                                round,
                                missing_count
                            );

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                debug!(
                                    "Header {} (round {}) already in pending, skipping duplicate sync request",
                                    header_id,
                                    round
                                );
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to when all
                            // its parents are in the store.
                            let wait_for: Vec<(Vec<u8>, Store)> = missing
                                .iter()
                                .map(|(digest, worker_id)| {
                                    let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                                    (key.to_vec(), self.store.clone())
                                })
                                .collect();
                            let wait_for_count = wait_for.len();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id.clone(), (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Ensure we didn't already send a sync request for these parents.
                            let mut requires_sync = HashMap::new();
                            for (digest, worker_id) in missing.into_iter() {
                                self.batch_requests.entry(digest.clone()).or_insert_with(|| {
                                    requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                                    round
                                });
                            }
                            for (worker_id, digests) in requires_sync {
                                let address = self.committee
                                    .worker(&author, &worker_id)
                                    .expect("Author of valid header is not in the committee")
                                    .primary_to_worker;
                                debug!(
                                    "Sending batch sync request for header {} (round {}): requesting {} batch(es) from worker {} at {}",
                                    header_id,
                                    round,
                                    digests.len(),
                                    worker_id,
                                    address
                                );
                                let message = PrimaryWorkerMessage::Synchronize(digests, author);
                                let bytes = bincode::serialize(&message)
                                    .expect("Failed to serialize batch sync request");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                            debug!(
                                "Header {} (round {}) added to waiter pool, waiting for {} batch(es) to arrive",
                                header_id,
                                round,
                                wait_for_count
                            );
                        }

                        WaiterMessage::SyncParents(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

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
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
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

                () = &mut timer => {
                    // Reschedule the timer.
                    timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
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
                self.batch_requests.retain(|_, r| r > &mut gc_round);
            }
        }
    }
}
