# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0,
                 default_client_size=None, default_client_rates=None):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            if default_client_size is None or default_client_rates is None:
                raise ParseError(f'Failed to parse clients\' logs: {e}')

            rates = default_client_rates
            if not isinstance(rates, list):
                rates = [rates] * len(clients)
            if len(rates) != len(clients):
                raise ParseError('Failed to parse clients\' logs: mismatched fallback rates')

            Print.warn(
                'Client logs are missing the expected metadata; '
                'falling back to configured transaction size/rate values'
            )
            results = [
                (default_client_size, rates[i], None, None, 0, {})
                for i in range(len(clients))
            ]
        self.size, self.rate, self.start, self.client_end, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        (
            proposals,
            commits,
            header_proposals,
            header_commits,
            header_sizes,
            primary_end,
            self.configs,
            primary_ips,
        ) = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.header_proposals = self._merge_results([x.items() for x in header_proposals])
        self.header_commits = self._merge_results([x.items() for x in header_commits])
        self.header_sizes = self._merge_results([x.items() for x in header_sizes])
        self.primary_end = primary_end

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, worker_end = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.worker_end = worker_end

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[+([^\] \[]+) [^\]]*\] Start sending transactions', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[+([^\] \[]+) [^\]]*\] Sending sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}
        end = max(samples.values()) if samples else self._last_timestamp(log)

        return size, rate, start, end, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[+([^\] \[]+) [^\]]*\] Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[+([^\] \[]+) [^\]]*\] Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        tmp = findall(
            r'\[+([^\] \[]+) [^\]]*\] VERTEX_SIZE round=(\d+) node=(\d+) header=[^ ]+ '
            r'vertex_bytes=\d+ payload_bytes=(\d+) payload_entries=\d+ payload_txs=\d+',
            log,
        )
        header_proposals = {(
            int(round),
            int(node),
        ): self._to_posix(t) for t, round, node, _payload_bytes in tmp}
        header_sizes = {
            (int(round), int(node)): int(payload_bytes)
            for _t, round, node, payload_bytes in tmp
        }

        tmp = findall(
            r'\[+([^\] \[]+) [^\]]*\] DAG_COMMITTED path=[^ ]+ round=(\d+) node=(\d+) digest=[^ ]+',
            log,
        )
        tmp = [((int(round), int(node)), self._to_posix(t)) for t, round, node in tmp]
        header_commits = self._merge_results([tmp])
        end = self._last_timestamp(log)

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, header_proposals, header_commits, header_sizes, end, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        end = self._last_timestamp(log)

        return sizes, samples, ip, end

    def _to_posix(self, string):
        normalized = string.strip().lstrip('[').rstrip(']')
        x = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _last_timestamp(self, log):
        tmp = findall(r'\[+([^\] \[]+) [^\]]*\]', log)
        if not tmp:
            return None
        return self._to_posix(tmp[-1])

    def _consensus_throughput(self):
        if not self.commits and not self.header_commits:
            return 0, 0, 0

        if self.sizes:
            start, end = min(self.proposals.values()), max(self.commits.values())
            bytes = sum(self.sizes.values())
        else:
            committed_headers = [
                key for key in self.header_commits if key in self.header_sizes
            ]
            if not committed_headers or not self.header_proposals:
                return 0, 0, 0
            start = min(self.header_proposals.values())
            end = max(self.header_commits[key] for key in committed_headers)
            bytes = sum(self.header_sizes[key] for key in committed_headers)

        duration = end - start
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        if not latency:
            latency = [
                c - self.header_proposals[k]
                for k, c in self.header_commits.items()
                if k in self.header_proposals
            ]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits and not self.header_commits:
            return 0, 0, 0
        start_candidates = [x for x in self.start if x is not None]

        if self.sizes:
            start = min(start_candidates) if start_candidates else min(self.proposals.values())
            end = max(self.commits.values())
            bytes = sum(self.sizes.values())
        else:
            committed_headers = [
                key for key in self.header_commits if key in self.header_sizes
            ]
            if not committed_headers:
                return 0, 0, 0
            fallback_start = min(self.header_proposals.values()) if self.header_proposals else None
            start = min(start_candidates) if start_candidates else fallback_start
            if start is None:
                return 0, 0, 0
            end = max(self.header_commits[key] for key in committed_headers)
            bytes = sum(self.header_sizes[key] for key in committed_headers)

        duration = end - start
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def _execution_duration(self):
        start_candidates = [x for x in self.start if x is not None]
        if start_candidates:
            start = min(start_candidates)
        else:
            client_end_candidates = []
            start = None

        client_end_candidates = [x for x in self.client_end if x is not None]
        if start is not None and client_end_candidates:
            return max(client_end_candidates) - start

        if start is None:
            if self.header_proposals:
                start = min(self.header_proposals.values())
            elif self.proposals:
                start = min(self.proposals.values())
            else:
                return 0

        end_candidates = [x for x in (list(self.primary_end) + list(self.worker_end)) if x is not None]
        if not end_candidates:
            return 0
        return max(end_candidates) - start

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        execution_duration = self._execution_duration()

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(execution_duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' Effective measurement window: {round(duration):,} s\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0, default_client_size=None,
                default_client_rates=None):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(
            clients,
            primaries,
            workers,
            faults=faults,
            default_client_size=default_client_size,
            default_client_rates=default_client_rates,
        )
