#!/usr/bin/env python3
import json
import shutil
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentTypeError, BooleanOptionalAction
from dataclasses import dataclass
import multiprocessing
from os import path
from pathlib import Path
import statistics
from subprocess import check_output
import sys
from datetime import datetime
from typing import Any, Literal, Mapping

import pandas as pd
import matplotlib.pyplot as plt


VERBOSITY_LEVEL = 0
FAIL_FAST = False

def print_debug(*args, required_verbosity_level: int, **kwargs) -> None:
    if VERBOSITY_LEVEL >= required_verbosity_level:
        print(*args, **kwargs)


def print_error(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def output(*args, required_verbosity_level, **kwargs) -> None:
    print_debug(*args, required_verbosity_level=required_verbosity_level, **kwargs)
    # print(*args, file=output_file, **kwargs)


def rm_dir(path: str) -> None:
    dirpath = Path(path)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


def enquote(x: str, /, *, quoatation_character='"') -> str:
    return f'{quoatation_character}{x}{quoatation_character}'


def read_json(path: str) -> dict:
    with open(path) as file:
        return json.load(file)


@dataclass(frozen=True)
class Configuration:
    output_directory: str
    build_path: str
    tmp_path: str
    scale_factor: float
    encoding_under_test: str
    time_limit: int
    verbosity_level: int
    skip_benchmarks: bool


class DictConvertible(ABC):
    @abstractmethod
    def as_dict(self) -> dict:
        ...


@dataclass(frozen=True, slots=True)
class Runtime(DictConvertible):
    benchmark_name: str
    items_per_second: float

    def as_dict(self) -> dict:
        return {
            'benchmark_name': self.benchmark_name,
            'items_per_second': self.items_per_second,
        }


@dataclass(slots=True)
class Runtimes(DictConvertible):
    runtimes: list[Runtime]

    def as_dict(self) -> dict:
        return {
            'runtimes': list(runtime.as_dict() for runtime in self.runtimes)
        }


@dataclass(frozen=True, slots=True)
class MemoryConsumption(DictConvertible):
    column_name: str
    column_type: str
    memory_consumption: int

    def as_dict(self) -> dict:
        return {
            'column_name': self.column_name,
            'column_type': self.column_type,
            'memory_consumption': self.memory_consumption,
        }


@dataclass(slots=True)
class Metrics(DictConvertible):
    memory_consumptions: list[MemoryConsumption]
    
    def as_dict(self) -> dict:
        return {
            'memory_consumption': list(consumption.as_dict() for consumption in self.memory_consumptions)
        }

    def min(self) -> int:
        return min(map(lambda x: x.memory_consumption, self.memory_consumptions))

    def max(self) -> int:
        return max(map(lambda x: x.memory_consumption, self.memory_consumptions))

    def average(self) -> float:
        return statistics.fmean(map(lambda x: x.memory_consumption, self.memory_consumptions))

    def median(self) -> float:
        return statistics.median(map(lambda x: x.memory_consumption, self.memory_consumptions))


def plot(results: dict[str, list], *, title: str, yaxis: str, path: str, figsize: tuple[int, int]=(15, 10)) -> None:
    f, axiis = plt.subplots(1, 1, figsize=figsize)
    # data=pd.DataFrame(data=results)
    data = pd.DataFrame.from_dict(results, orient='index')
    data = data.transpose()
    print_debug(data, required_verbosity_level=3)
    if data.empty:
        print_error('Data Frame is empty; no result data to show!')
        return
    data.plot(
        kind='box',
        ax=axiis,
        title=title,
        xlabel='Encodings',
        ylabel=f'{yaxis} (Logarithmic Scale)',
        logy=True,
    )
    f.tight_layout()
    f.savefig(path)


class Benchmark(ABC):
    name = 'Benchmark'
    _config: Configuration

    # These encodings are supported by default for string types.
    _encodings = [
        'Unencoded',
        'Dictionary',
        'RunLength',
        'FixedStringDictionary',
        'LZ4'
    ]

    @staticmethod
    def create(name: str, *args: Any, **kwargs: Any) -> 'Benchmark':
        classes = {
            'hyriseBenchmarkTPCH': TPCHBenchmark,
            'hyriseBenchmarkTPCDS': TPCDSBenchmark,
            'hyriseBenchmarkJoinOrder': JoinOrderBenchmark,
            'hyriseBenchmarkStarSchema': StarSchemaBenchmark
        }
        return classes[name](*args, **kwargs)

    """
    Create a config file as depicted in the `--full_help` of the benchmarks and return its path.
    """
    def _write_encoding_config_file(self, threading: Literal['ST', 'MT'], encoding: str, metrics: bool) -> str:
        config_path = path.join(self._config.tmp_path, 'config', f'{self.name}-{threading}-{encoding}-{metrics}.json')
        config_contents = {
            'default': {
                'encoding': 'Dictionary'
            },
            'type': {
                'string': {
                    'encoding': encoding
                }
            }
        }
        with open(config_path, mode='w') as config:
            json.dump(config_contents, config)
        return config_path

    def run(self, config: Configuration) -> Mapping[str, Runtimes] | None:
        try:
            self._config = config
            output(f'## Benchmark {self.name}:', required_verbosity_level=1)
            threading_options: list[Literal['ST', 'MT']] = ['ST', 'MT']
            for threading in threading_options:
                output(f'### {"Single Thread" if threading == "ST" else "Mulitple Threads"}', required_verbosity_level=1)
                result_jsons = [self._run(threading, encoding, False) for encoding in self._encodings]
                test_json = self._run(threading, self._config.encoding_under_test, False)
                times = {
                    encoding: self._compare_run(result_json)
                    for result_json, encoding
                    in zip(result_jsons, self._encodings)
                }
                times[self._config.encoding_under_test] = self._compare_run(test_json)
                plot_path = path.join(self._config.output_directory, 'runtime', 'plots', f'{self.name}-{threading}.png')
                Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
                times_to_plot = {
                    encoding: list(map(lambda x: x.items_per_second, runtimes.runtimes))
                    for encoding, runtimes
                    in times.items()
                }
                plot(times_to_plot, title=f'Items per second for {self.name}', yaxis='Items per second of individual benchmark tests', path=plot_path)
                # Dump raw data
                raw_file_path = path.join(self._config.output_directory, 'runtime', 'raw', f'{self.name}-{threading}.json')
                Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
                with open(raw_file_path, 'w') as f:
                    raw_times = {
                        encoding: runtimes.as_dict()
                        for encoding, runtimes
                        in times.items()
                    }
                    json.dump(raw_times, f)
                # for (encoding, json) in zip(self._encodings, result_jsons):
                #     check_command = ['./scripts/compare_benchmarks.py', json, test_json]
                #     compare_result = check_output(check_command)
                #     output(f'Result for {encoding} vs. {self._config.encoding_under_test}:', required_verbosity_level=3)
                #     output(compare_result.decode(encoding='utf-8'), required_verbosity_level=3)
                return times
        except Exception as ex:
            if FAIL_FAST:
                raise ex
            error_message = f'Skipped {self.name} due to error {ex}.'
            print_error(error_message)
            output(error_message, required_verbosity_level=4)
            return None

    def _compare_run(self, json: str) -> Runtimes:
        json_file = read_json(json)
        runtimes = Runtimes([])
        for benchmark in json_file['benchmarks']:
            name = benchmark['name']
            # successful_runs = benchmark['successful_runs']
            duration = benchmark['items_per_second']
            # duration = statistics.mean(float(run['duration']) for run in successful_runs)
            runtime = Runtime(name, duration)
            runtimes.runtimes.append(runtime)
        # return list(map(lambda x: x.runtime, runtimes.runtimes))
        return runtimes

    def compare_metrics(self, config: Configuration) -> Mapping[str, Metrics] | None:
        try:
            self._config = config
            output(f'### Collecting Metrics for {self.name}:', required_verbosity_level=1)
            result_jsons = [self._run('ST', encoding, True) for encoding in self._encodings]
            test_json = self._run('ST', self._config.encoding_under_test, True)
            metrics = {encoding: self._compare_metrics(result_json) for result_json, encoding in zip(result_jsons, self._encodings)}
            metrics[self._config.encoding_under_test]= self._compare_metrics(test_json)
            plot_path = path.join(self._config.output_directory, 'metrics', 'plots', f'{self.name}.png')
            Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
            metrics_to_plot: dict[str, list[int]] = {}
            for encoding, metric in metrics.items():
                metrics_to_plot[f'{encoding} (String)'] = list(map(lambda x: x.memory_consumption, filter(lambda x: x.column_type == 'string', metric.memory_consumptions)))
                metrics_to_plot[f'{encoding} (All)'] = list(map(lambda x: x.memory_consumption, metric.memory_consumptions))
            plot(metrics_to_plot, title=f'Sizes for {self.name}', yaxis='Size of Segments', path=plot_path, figsize=(22, 10))
            # Dump raw data
            raw_file_path = path.join(self._config.output_directory, 'metrics', 'raw', f'{self.name}.json')
            Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(raw_file_path, 'w') as f:
                raw_metrics = {
                        encoding: metric.as_dict()
                        for encoding, metric
                        in metrics.items()
                    }
                json.dump(raw_metrics, f)
            # for encoding, json in zip(self._encodings, result_jsons):
            #     self._compare_metrics(json, test_json, encoding, self._config.encoding_under_test)
            return metrics
        except Exception as ex:
            if FAIL_FAST:
                raise ex
            error_message = f'Skipped {self.name} due to error {ex}.'
            print_error(error_message)
            output(error_message, required_verbosity_level=4)
            return None

    # def _compare_metrics(self, reference_json: str, test_json: str, reference_encoding: str, test_encoding: str) -> 'Metrics':
    def _compare_metrics(self, json: str) -> Metrics:
        json_file = read_json(json)
        # test = read_json(test_json)
        metrics = Metrics([])
        # test_metrics = Metrics([])
        for segment in json_file['segments']:
        # for ref_segment, test_segment in zip(reference['segments'], test['segments']):
            column_type = segment['column_data_type']
            column_name = segment['column_name']
            # # Only look at string columns
            # if column_type != 'string':
            #     continue
            metrics.memory_consumptions.append(MemoryConsumption(column_name, column_type, segment["estimated_size_in_bytes"]))
            # test_metrics.memory_consumptions.append(MemoryConsumption(column_name, column_type, test_segment["estimated_size_in_bytes"]))
        # output(f'For reference encoding {reference_encoding}: {{ min: {reference_metrics.min()}, max: {reference_metrics.max()}, average: {reference_metrics.average()}, median: {reference_metrics.median()} }}; ' +
        #        f'For test encoding {test_encoding}: {{ min: {test_metrics.min()}, max: {test_metrics.max()}, average: {test_metrics.average()}, median: {test_metrics.median()} }}', required_verbosity_level=2)
        # return list(map(lambda x: x.memory_consumption, metrics.memory_consumptions))
        return metrics

    def _run(self, threading: Literal['ST', 'MT'], encoding: str, metrics: bool) -> str:
        if self._config.skip_benchmarks:
            print_debug(f'Skipping benchmark {self.name} for encoding {encoding}.', required_verbosity_level=2)
            return self._output_path(threading, encoding, metrics)
        self._pre_run_cleanup()
        print_debug(f'Running benchmark {self.name} for encoding {encoding}.', required_verbosity_level=1)
        st_command = self._get_arguments(threading, encoding, metrics)
        print_debug(f'Command: `{" ".join(map(lambda x: enquote(x), st_command))}`', required_verbosity_level=2)
        st_output = check_output(st_command)
        print_debug(f'Output of above command: `{st_output}`', required_verbosity_level=3)
        return self._output_path(threading, encoding, metrics)

    def _get_arguments(self, threading: Literal['ST', 'MT'], encoding: str, metrics: bool) -> list[str]:
        encoding_config_path = self._write_encoding_config_file(threading, encoding, metrics)
        arguments = [self._path, '-o', self._output_path(threading, encoding, metrics), '-e', encoding_config_path]
        if threading == 'MT':
            arguments += ['--scheduler', '--clients', str(multiprocessing.cpu_count() // 4), '--mode=Shuffled']
            # Multithreaded runs need longer times to be meaningful. Default to 20 minutes.
            arguments += ['-t', str(self._config.time_limit * 20)]
        else:
            arguments += ['-t', str(self._config.time_limit)]
        if metrics:
            arguments += ['--metrics', '-r', '1']
        return arguments

    @abstractmethod
    def _pre_run_cleanup(self) -> None:
        ...

    @property
    def _path(self) -> str:
        ...

    def _output_path(self, threading: Literal['ST'] | Literal['MT'], encoding: str, metrics: bool) -> str:
        return path.join(self._config.tmp_path, f'{self.name}-{threading}-{encoding}-{metrics}.json')


class TPCHBenchmark(Benchmark):
    name = 'tpch'

    def _get_arguments(self, threading: Literal['ST', 'MT'], encoding: str, metrics: bool) -> list[str]:
        return super()._get_arguments(threading, encoding, metrics) + ['-s', str(self._config.scale_factor)]

    def _pre_run_cleanup(self) -> None:
        rm_dir('tpch_cached_tables')

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkTPCH')

    
class TPCDSBenchmark(Benchmark):
    name = 'tpcds'

    def _get_arguments(self, threading: Literal['ST', 'MT'], encoding: str, metrics: bool) -> list[str]:
        # TPC-DS only supports integer scales
        return super()._get_arguments(threading, encoding, metrics) + ['-s', str(max(1, int(self._config.scale_factor)))]

    def _pre_run_cleanup(self) -> None:
        rm_dir('tpcds_cached_tables')

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkTPCDS')


class JoinOrderBenchmark(Benchmark):
    name = 'job'

    def _pre_run_cleanup(self) -> None:
        rm_dir('imdb_data')

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkJoinOrder')


class StarSchemaBenchmark(Benchmark):
    name = 'ssb'

    def _pre_run_cleanup(self) -> None:
        rm_dir('imdb_data')

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkStarSchema')


def parse_arguments() -> Configuration:
    global VERBOSITY_LEVEL
    global FAIL_FAST
    def check_positive(value) -> int:
        ivalue = int(value)
        if ivalue <= 0:
            raise ArgumentTypeError("%s is an invalid positive int value" % value)
        return ivalue
    parser = ArgumentParser()
    parser.add_argument(
        '-o',
        '--output-directory',
        dest='output_directory',
        type=str,
        required=True,
        help='The directory where the output should be stored.'
    )
    parser.add_argument(
        '-b',
        '--build-path',
        dest='build_path',
        type=str,
        required=True,
        help='Path where the executables to benchmark are located.'
    )
    parser.add_argument(
        '-s',
        '--scale-factor',
        dest='scale_factor',
        type=float,
        required=True,
        help='The scale factor to pass to the benchmarks that support scaling. Note that this number might get rounded or ignored if necessary for a benchmark.'
    )
    parser.add_argument(
        '-p',
        '--tmp-path',
        dest='tmp_path',
        type=str,
        required=False,
        default='tmp',
        help='The directory where the benchmark result files will be stored.'
    )
    parser.add_argument(
        '-e',
        '--encoding-under-test',
        dest='encoding',
        type=str,
        required=True,
        help='The name of the encoding to compare against built-in encodings.'
    )
    parser.add_argument(
        '-t',
        '--timeout',
        dest='timeout',
        type=check_positive,
        required=False,
        default=60,
        help='The timeout in seconds to pass to the benchmarks. Defaults to 60.'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest='verbosity_level',
        required=False,
        default=0,
        action='count',
        help='Verbosity level. Supports multiple ocurrences (like `-vvv`) to increase verbosity.'
    )
    parser.add_argument(
        '--fail-fast',
        dest='fail_fast',
        action=BooleanOptionalAction,
        required=False,
        default=False,
        help='Whether to fail early when an error occurs'
    )
    parser.add_argument(
        '--skip-benchmarks',
        dest='skip_benchmarks',
        action=BooleanOptionalAction,
        required=False,
        default=False,
        help='Whether to skip running the benchmarks and instead using old data.'
    )
    namespace = parser.parse_args()
    if namespace.verbosity_level > 0:
        VERBOSITY_LEVEL = namespace.verbosity_level
        print_debug(f'Verbosity Mode enabled on level {VERBOSITY_LEVEL}.', required_verbosity_level=VERBOSITY_LEVEL)
    if namespace.fail_fast:
        FAIL_FAST = True
        print_debug(f'Fail Fast Mode enabled', required_verbosity_level=2)
    now = datetime.now()
    now_date = f'{now.year}{now.month:02d}{now.day:02d}'
    now_time = f'{now.hour}{now.minute:02d}{now.second:02d}'
    return Configuration(
        output_directory=path.join(namespace.output_directory, f'run-{now_date}-{now_time}'),
        build_path=namespace.build_path,
        tmp_path=namespace.tmp_path,
        scale_factor=namespace.scale_factor,
        encoding_under_test=namespace.encoding,
        time_limit=namespace.timeout,
        verbosity_level=namespace.verbosity_level,
        skip_benchmarks=namespace.skip_benchmarks)


def scale_factor_for_benchmark(benchmark: str, scale_factor: float) -> float:
    if benchmark == 'hyriseBenchmarkTPCDS':
        return max(scale_factor, 1)
    return scale_factor


def locate_benchmarks(benchmarks: list[str], config: Configuration) -> list[Benchmark]:
    benchmark_objects: list[Benchmark] = []
    for benchmark in benchmarks:
        benchmark_path = path.join(config.build_path, benchmark)
        if not path.isfile(benchmark_path):
            exit(f'Cannot locate {benchmark} at {benchmark_path}!')
        benchmark_objects.append(Benchmark.create(benchmark))
    return benchmark_objects


def plot_stats(stats: dict[str, tuple[Mapping[str, Runtimes], Mapping[str, Metrics]]], *, figsize: tuple[int, int]=(15, 10)) -> None:
    f, axiis = plt.subplots(2, 2, figsize=figsize)
    # data=pd.DataFrame(data=results)
    data = pd.DataFrame.from_dict(stats, orient='index')
    data = data.transpose()
    print_debug(data, required_verbosity_level=3)
    if data.empty:
        print_error('Data Frame is empty; no result data to show!')
        return
    data.plot(
        kind='box',
        ax=axiis,
        title=title,
        xlabel='Encodings',
        ylabel=f'{yaxis} (Logarithmic Scale)',
        logy=True,
    )
    f.tight_layout()
    f.savefig(path)


def main():
    global output_file
    config = parse_arguments()
    Path(config.tmp_path).mkdir(parents=True, exist_ok=True)
    (Path(config.tmp_path) / Path('config')).mkdir(parents=True, exist_ok=True)
    Path(config.output_directory).mkdir(parents=True, exist_ok=True)
    if True:
    # with open(config.output_file, 'w+') as output_file:
        print_debug(f'Running benchmark comparing {config.encoding_under_test} Encoding against built-in encodings.', required_verbosity_level=1)
        benchmarks_names = ['hyriseBenchmarkTPCH', 'hyriseBenchmarkTPCDS', 'hyriseBenchmarkJoinOrder', 'hyriseBenchmarkStarSchema']
        benchmarks = locate_benchmarks(benchmarks_names, config)
        stats: dict[str, tuple[Mapping[str, Runtimes], Mapping[str, Metrics]]] = {}
        for benchmark in benchmarks:
            runtimes = benchmark.run(config)
            metrics = benchmark.compare_metrics(config)
            if runtimes is not None and metrics is not None:
                stats[benchmark.name] = (runtimes, metrics)
        # plot_stats(stats)


if __name__ == '__main__':
    main()
