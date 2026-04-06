# Copyright(C) Facebook, Inc. and its affiliates.
import csv
import json
import os
import re
from datetime import datetime
from glob import glob
from os.path import join


class BenchError(Exception):
    def __init__(self, message, error):
        assert isinstance(error, Exception)
        self.message = message
        self.cause = error
        super().__init__(message)


class PathMaker:
    RUN_DIR_ENV = 'MANTA_RUN_DIR'
    LATEST_RUN_FILE = '.latest_run'

    @staticmethod
    def binary_path():
        return join('..', 'target', 'release')

    @staticmethod
    def node_crate_path():
        return join('..', 'node')

    @staticmethod
    def committee_file():
        return '.committee.json'

    @staticmethod
    def parameters_file():
        return '.parameters.json'

    @staticmethod
    def key_file(i):
        assert isinstance(i, int) and i >= 0
        return f'.node-{i}.json'

    @staticmethod
    def db_path(i, j=None):
        assert isinstance(i, int) and i >= 0
        assert (isinstance(j, int) and i >= 0) or j is None
        worker_id = f'-{j}' if j is not None else ''
        return f'.db-{i}{worker_id}'

    @staticmethod
    def logs_path():
        return join(PathMaker.output_path(), 'logs')

    @staticmethod
    def primary_log_file(i):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(), f'primary-{i}.log')

    @staticmethod
    def worker_log_file(i, j):
        assert isinstance(i, int) and i >= 0
        assert isinstance(j, int) and i >= 0
        return join(PathMaker.logs_path(), f'worker-{i}-{j}.log')

    @staticmethod
    def client_log_file(i, j):
        assert isinstance(i, int) and i >= 0
        assert isinstance(j, int) and i >= 0
        return join(PathMaker.logs_path(), f'client-{i}-{j}.log')

    @staticmethod
    def results_path():
        return PathMaker.output_path()

    @staticmethod
    def result_file(faults, nodes, workers, collocate, rate, tx_size):
        return join(
            PathMaker.results_path(),
            f'bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        )

    @staticmethod
    def summary_file():
        return join(PathMaker.results_path(), 'summary.txt')

    @staticmethod
    def latency_csv_file():
        return join(PathMaker.results_path(), 'latency.csv')

    @staticmethod
    def final_dag_file():
        return join(PathMaker.results_path(), 'final_dag.txt')

    @staticmethod
    def solid_step_vertices_csv_file():
        return join(PathMaker.results_path(), 'solid_step_vertices.csv')

    @staticmethod
    def plots_path():
        return join(PathMaker.base_results_path(), 'plots')

    @staticmethod
    def agg_file(type, faults, nodes, workers, collocate, rate, tx_size, max_latency=None):
        if max_latency is None:
            name = f'{type}-bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        else:
            name = f'{type}-{max_latency}-bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        return join(PathMaker.plots_path(), name)

    @staticmethod
    def plot_file(name, ext):
        return join(PathMaker.plots_path(), f'{name}.{ext}')

    @staticmethod
    def base_results_path():
        return 'manta_result'

    @staticmethod
    def latest_run_file():
        return join(PathMaker.base_results_path(), PathMaker.LATEST_RUN_FILE)

    @staticmethod
    def output_path():
        return PathMaker.current_run_path() or PathMaker.base_results_path()

    @staticmethod
    def current_run_path():
        run_dir = os.environ.get(PathMaker.RUN_DIR_ENV)
        if run_dir:
            return run_dir

        latest_run_file = PathMaker.latest_run_file()
        if os.path.exists(latest_run_file):
            with open(latest_run_file, 'r') as f:
                run_dir = f.read().strip()
            return run_dir or None
        return None

    @staticmethod
    def activate_run_directory(run_dir):
        assert isinstance(run_dir, str) and run_dir
        os.makedirs(run_dir, exist_ok=True)
        os.environ[PathMaker.RUN_DIR_ENV] = run_dir

        os.makedirs(PathMaker.base_results_path(), exist_ok=True)
        with open(PathMaker.latest_run_file(), 'w') as f:
            f.write(run_dir)
        return run_dir

    @staticmethod
    def _sanitize_label(label):
        label = re.sub(r'[^A-Za-z0-9._-]+', '-', label.strip())
        return label.strip('-') or 'run'

    @staticmethod
    def create_run_directory(label='run'):
        safe_label = PathMaker._sanitize_label(label)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        base_dir = PathMaker.base_results_path()
        os.makedirs(base_dir, exist_ok=True)

        run_dir = join(base_dir, f'{timestamp}_{safe_label}')
        counter = 1
        while os.path.exists(run_dir):
            counter += 1
            run_dir = join(base_dir, f'{timestamp}_{safe_label}_{counter}')

        PathMaker.activate_run_directory(run_dir)
        with open(join(run_dir, 'run_metadata.json'), 'w') as f:
            json.dump(
                {
                    'created_at_utc': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
                    'label': safe_label,
                    'run_dir': run_dir,
                },
                f,
                indent=2,
            )
            f.write('\n')
        return run_dir

    @staticmethod
    def all_result_files():
        patterns = [
            join(PathMaker.base_results_path(), 'bench-*.txt'),
            join(PathMaker.base_results_path(), '*', 'bench-*.txt'),
        ]
        files = []
        for pattern in patterns:
            files.extend(glob(pattern))
        return sorted(set(files))

    @staticmethod
    def export_run_artifacts():
        artifacts = {}
        final_dag = PathMaker.export_final_dag()
        if final_dag:
            artifacts['final_dag'] = final_dag

        solid_step_csv = PathMaker.export_solid_step_vertices_csv()
        if solid_step_csv:
            artifacts['solid_step_vertices_csv'] = solid_step_csv

        return artifacts

    @staticmethod
    def export_final_dag(log_files=None, output_file=None):
        log_files = log_files or sorted(glob(join(PathMaker.logs_path(), 'primary-*.log')))
        round_line_re = re.compile(r"\bRound\s+(\d+):\s+(.*)")
        latest = {}

        for path in log_files:
            if not os.path.exists(path):
                continue
            with open(path, 'r', errors='replace') as f:
                for line in f:
                    match = round_line_re.search(line)
                    if not match:
                        continue
                    round_num = int(match.group(1))
                    latest[round_num] = f"Round {round_num}: {match.group(2).strip()}"

        if not latest:
            return None

        output_file = output_file or PathMaker.final_dag_file()
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as f:
            for round_num in sorted(latest):
                f.write(latest[round_num])
                f.write('\n')
        return output_file

    @staticmethod
    def export_solid_step_vertices_csv(log_files=None, output_file=None):
        log_files = log_files or sorted(glob(join(PathMaker.logs_path(), 'primary-*.log')))
        line_re = re.compile(
            r"\[(?P<ts>[^]]+)\s+DEBUG\s+primary::(?:proposer|aggregators)\]\s+"
            r"Current round:\s+(?P<round>\d+),\s+"
            r"(?:(?:The number of (?:merged )?solid-step vertices is)\s+|solid_step_vertices=)"
            r"(?P<count>\d+)"
        )
        max_by_key = {}

        for path in log_files:
            if not os.path.exists(path):
                continue
            primary_name = os.path.splitext(os.path.basename(path))[0]
            with open(path, 'r', errors='replace') as f:
                for line in f:
                    match = line_re.search(line)
                    if not match:
                        continue
                    round_num = int(match.group('round'))
                    count = int(match.group('count'))
                    ts = match.group('ts')
                    key = (primary_name, round_num)
                    existing = max_by_key.get(key)
                    if existing is None or count > existing['solid_step_vertices']:
                        max_by_key[key] = {
                            'primary': primary_name,
                            'timestamp': ts,
                            'round': round_num,
                            'solid_step_vertices': count,
                        }
                    elif count == existing['solid_step_vertices'] and ts > existing['timestamp']:
                        existing['timestamp'] = ts

        if not max_by_key:
            return None

        output_file = output_file or PathMaker.solid_step_vertices_csv_file()
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['primary', 'timestamp', 'round', 'solid_step_vertices'],
            )
            writer.writeheader()
            for row in sorted(
                max_by_key.values(),
                key=lambda item: (item['round'], item['primary'], item['timestamp']),
            ):
                writer.writerow(row)
        return output_file


class Color:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Print:
    @staticmethod
    def heading(message):
        assert isinstance(message, str)
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def info(message):
        assert isinstance(message, str)
        print(message)

    @staticmethod
    def warn(message):
        assert isinstance(message, str)
        print(f'{Color.BOLD}{Color.WARNING}WARN{Color.END}: {message}')

    @staticmethod
    def error(e):
        assert isinstance(e, BenchError)
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {e}\n')
        causes, current_cause = [], e.cause
        while isinstance(current_cause, BenchError):
            causes += [f'  {len(causes)}: {e.cause}\n']
            current_cause = current_cause.cause
        causes += [f'  {len(causes)}: {type(current_cause)}\n']
        causes += [f'  {len(causes)}: {current_cause}\n']
        print(f'Caused by: \n{"".join(causes)}\n')


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=30, fill='█', print_end='\r'):
    total = len(iterable)

    def printProgressBar(iteration):
        formatter = '{0:.'+str(decimals)+'f}'
        percent = formatter.format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

    printProgressBar(0)
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    print()
