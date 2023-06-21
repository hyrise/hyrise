#!/usr/bin/env python3

from abc import ABC, abstractmethod
from argparse import ArgumentParser, BooleanOptionalAction
from dataclasses import dataclass
import multiprocessing
from os import path
from subprocess import check_output
import sys
from typing import Any, Literal


DEBUG_MODE = False

def print_debug(*args, **kwargs) -> None:
    if DEBUG_MODE:
        print(*args, **kwargs)


def print_error(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def output(*args, **kwargs) -> None:
    print(*args, file=output_file, **kwargs)


@dataclass(frozen=True)
class Configuration:
    output_file: str
    build_path: str
    tmp_path: str
    scale_factor: float
    encoding_under_test: str
    time_limit: int
    debug: bool


class Benchmark(ABC):
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
    
    def run(self, config: Configuration) -> None:
        try:
            self._config = config
            output(f'## Benchmark {self.name}:')
            for threading in ['ST', 'MT']:
                output(f'### {"Single Thread" if threading == "ST" else "Mulitple Threads"}')
                result_jsons = [self._run(threading, encoding) for encoding in self._encodings]
                test_json = self._run(threading, self._config.encoding_under_test)
                for (encoding, json) in zip(self._encodings, result_jsons):
                    check_command = ['./scripts/compare_benchmarks.py', json, test_json]
                    compare_result = check_output(check_command)
                    output(f'Result for {encoding} vs. {self._config.encoding_under_test}:')
                    output(compare_result.decode(encoding='utf-8'))
        except Exception as ex:
            error_message = f'Skipped {self.name} due to error {ex}.'
            print_error(error_message)
            output(error_message)

    @abstractmethod
    def _run(self, threading: Literal['ST', 'MT'], encoding: str) -> str:
        pass

    @property
    def _path(self) -> str:
        pass

    @abstractmethod
    def _output_path(self, threading: Literal['ST'] | Literal['MT']) -> str:
        pass


class TPCHBenchmark(Benchmark):
    name = 'tpch'
    def _run(self, threading: Literal['ST', 'MT'], encoding: str) -> str:
        if threading == 'ST':
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-s', str(self._config.scale_factor), '-e', encoding]
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path
        else:
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-s', str(self._config.scale_factor), '-e', encoding,
                          '--scheduler', '--clients', str(multiprocessing.cpu_count() // 4), '--mode=Shuffled']
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path


    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkTPCH')

    def _output_path(self, threading: Literal['ST'] | Literal['MT'], encoding: str) -> str:
        return path.join(self._config.tmp_path, f'{self.name}-{threading}-{encoding}.json')

    
class TPCDSBenchmark(Benchmark):
    name = 'tpcds'
    def _run(self, threading: Literal['ST', 'MT'], encoding: str) -> None:#
        # TPC-DS only supports integer scales
        scaling_factor = max(1, int(self._config.scale_factor))
        if threading == 'ST':
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-s', str(scaling_factor), '-e', encoding]
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path
        else:
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-s', str(scaling_factor), '-e', encoding,
                          '--scheduler', '--clients', str(multiprocessing.cpu_count() // 4), '--mode=Shuffled']
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkTPCDS')

    def _output_path(self, threading: Literal['ST'] | Literal['MT'], encoding: str) -> str:
        return path.join(self._config.tmp_path, f'{self.name}-{threading}-{encoding}.json')


class JoinOrderBenchmark(Benchmark):
    name = 'job'
    def _run(self, threading: Literal['ST', 'MT'], encoding: str) -> str:
        if threading == 'ST':
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-e', encoding]
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path
        else:
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-e', encoding,
                          '--scheduler', '--clients', str(multiprocessing.cpu_count() // 4), '--mode=Shuffled']
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkJoinOrder')

    def _output_path(self, threading: Literal['ST'] | Literal['MT'], encoding: str) -> str:
        return path.join(self._config.tmp_path, f'{self.name}-{threading}-{encoding}.json')


class StarSchemaBenchmark(Benchmark):
    name = 'ssb'
    def _run(self, threading: Literal['ST', 'MT'], encoding: str) -> None:
        if threading == 'ST':
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-e', encoding]
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path
        else:
            benchmark_path = self._path
            st_output_path = self._output_path(threading, encoding)
            st_command = [benchmark_path, '-o', st_output_path, '-t', str(self._config.time_limit), '-e', encoding,
                          '--scheduler', '--clients', str(multiprocessing.cpu_count() // 4), '--mode=Shuffled']
            print_debug(' '.join(st_command))
            st_output = check_output(st_command)
            print_debug(st_output)
            return st_output_path

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, 'hyriseBenchmarkStarSchema')

    def _output_path(self, threading: Literal['ST'] | Literal['MT'], encoding: str) -> str:
        return path.join(self._config.tmp_path, f'{self.name}-{threading}-{encoding}.json')


def parse_arguments() -> Configuration:
    global DEBUG_MODE
    parser = ArgumentParser()
    parser.add_argument(
        '-o',
        '--output-file',
        dest='output_file',
        type=str,
        required=True,
        help='The file where the output should be stored.'
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
        '-t',
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
        type=int,
        required=False,
        default=-1,
        help='The timeout in seconds to pass to the benchmarks. Defaults to -1, i.e. no timeout.'
    )
    parser.add_argument(
        '-d',
        '--debug',
        dest='debug',
        required=False,
        default=False,
        action=BooleanOptionalAction,
        help='Whether to activate debug mode (more verbose output to stdout).'
    )
    namespace = parser.parse_args()
    if namespace.debug:
        DEBUG_MODE = True
        print_debug('Debug Mode enabled.')
    return Configuration(
        output_file=namespace.output_file,
        build_path=namespace.build_path,
        tmp_path=namespace.tmp_path,
        scale_factor=namespace.scale_factor,
        encoding_under_test=namespace.encoding,
        time_limit=namespace.timeout,
        debug=namespace.debug)


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


def main():
    global output_file
    config = parse_arguments()
    with open(config.output_file, 'w+') as output_file:
        benchmarks_names = ['hyriseBenchmarkTPCH', 'hyriseBenchmarkTPCDS', 'hyriseBenchmarkJoinOrder', 'hyriseBenchmarkStarSchema']
        benchmarks = locate_benchmarks(benchmarks_names, config)
        for benchmark in benchmarks:
            benchmark.run(config)


if __name__ == '__main__':
    main()
