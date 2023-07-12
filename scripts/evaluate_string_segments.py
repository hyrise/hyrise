#!/usr/bin/env python3
import json
import shutil
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentTypeError
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
    return f"{quoatation_character}{x}{quoatation_character}"


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


def plot(
    results: Mapping[str, float], *, title: str, yaxis: str, path: str, figsize: tuple[int, int] = (15, 10)
) -> None:
    f, axiis = plt.subplots(1, 1, figsize=figsize)
    data = pd.DataFrame(data=results)
    print_debug(data, required_verbosity_level=3)
    if data.empty:
        print_error("Data Frame is empty; no result data to show!")
        return
    data.plot(
        kind="box",
        ax=axiis,
        title=title,
        xlabel="Encodings",
        ylabel=f"{yaxis} (Logarithmic Scale)",
        logy=True,
    )
    f.tight_layout()
    f.savefig(path)


class Benchmark(ABC):
    _config: Configuration

    # These encodings are supported by default for string types.
    _encodings = ["Unencoded", "Dictionary", "RunLength", "FixedStringDictionary", "LZ4"]

    @staticmethod
    def create(name: str, *args: Any, **kwargs: Any) -> "Benchmark":
        classes = {
            "hyriseBenchmarkTPCH": TPCHBenchmark,
            "hyriseBenchmarkTPCDS": TPCDSBenchmark,
            "hyriseBenchmarkJoinOrder": JoinOrderBenchmark,
            "hyriseBenchmarkStarSchema": StarSchemaBenchmark,
        }
        return classes[name](*args, **kwargs)

    def run(self, config: Configuration) -> None:
        try:
            self._config = config
            output(f"## Benchmark {self.name}:", required_verbosity_level=1)
            for threading in ["ST", "MT"]:
                output(
                    f'### {"Single Thread" if threading == "ST" else "Mulitple Threads"}', required_verbosity_level=1
                )
                result_jsons = [self._run(threading, encoding, False) for encoding in self._encodings]
                test_json = self._run(threading, self._config.encoding_under_test, False)
                times = {
                    encoding: self._compare_run(result_json)
                    for result_json, encoding in zip(result_jsons, self._encodings)
                }
                times[self._config.encoding_under_test] = self._compare_run(test_json)
                plot_path = path.join(self._config.output_directory, "runtime", "plots", f"{self.name}-{threading}.png")
                Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
                plot(
                    times,
                    title=f"Items per second for {self.name}",
                    yaxis="Items per second of individual benchmark tests",
                    path=plot_path,
                )
                # Dump raw data
                raw_file_path = path.join(
                    self._config.output_directory, "runtime", "raw", f"{self.name}-{threading}.json"
                )
                Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
                with open(raw_file_path, "w") as f:
                    json.dump(times, f)
                # for (encoding, json) in zip(self._encodings, result_jsons):
                #     check_command = ['./scripts/compare_benchmarks.py', json, test_json]
                #     compare_result = check_output(check_command)
                #     output(f'Result for {encoding} vs. {self._config.encoding_under_test}:', required_verbosity_level=3)
                #     output(compare_result.decode(encoding='utf-8'), required_verbosity_level=3)
        except Exception as ex:
            error_message = f"Skipped {self.name} due to error {ex}."
            print_error(error_message)
            output(error_message, required_verbosity_level=4)

    def _compare_run(self, json: str) -> list[float]:
        @dataclass(frozen=True)
        class Runtime:
            benchmark_name: str
            runtime: float

        @dataclass
        class Runtimes:
            runtimes: list[Runtime]

        json_file = read_json(json)
        runtimes = Runtimes([])
        for benchmark in json_file["benchmarks"]:
            name = benchmark["name"]
            # successful_runs = benchmark['successful_runs']
            duration = benchmark["items_per_second"]
            # duration = statistics.mean(float(run['duration']) for run in successful_runs)
            runtime = Runtime(name, duration)
            runtimes.runtimes.append(runtime)
        return list(map(lambda x: x.runtime, runtimes.runtimes))

    def compare_metrics(self, config: Configuration) -> None:
        try:
            self._config = config
            output(f"### Collecting Metrics for {self.name}:", required_verbosity_level=1)
            result_jsons = [self._run("ST", encoding, True) for encoding in self._encodings]
            test_json = self._run("ST", self._config.encoding_under_test, True)
            metrics = {
                encoding: self._compare_metrics(result_json)
                for result_json, encoding in zip(result_jsons, self._encodings)
            }
            metrics[self._config.encoding_under_test] = self._compare_metrics(test_json)
            plot_path = path.join(self._config.output_directory, "metrics", "plots", f"{self.name}.png")
            Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
            plot(metrics, title=f"Sizes for {self.name}", yaxis="Size of Segments", path=plot_path)
            # Dump raw data
            raw_file_path = path.join(self._config.output_directory, "metrics", "raw", f"{self.name}.json")
            Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(raw_file_path, "w") as f:
                json.dump(metrics, f)
            # for encoding, json in zip(self._encodings, result_jsons):
            #     self._compare_metrics(json, test_json, encoding, self._config.encoding_under_test)
        except Exception as ex:
            error_message = f"Skipped {self.name} due to error {ex}."
            print_error(error_message)
            output(error_message, required_verbosity_level=4)

    # def _compare_metrics(self, reference_json: str, test_json: str, reference_encoding: str, test_encoding: str) -> 'Metrics':
    def _compare_metrics(self, json: str) -> list[int]:
        @dataclass(frozen=True)
        class MemoryConsumption:
            column_name: str
            column_type: str
            memory_consumption: int

        @dataclass
        class Metrics:
            memory_consumptions: list[MemoryConsumption]

            def min(self) -> int:
                return min(map(lambda x: x.memory_consumption, self.memory_consumptions))

            def max(self) -> int:
                return max(map(lambda x: x.memory_consumption, self.memory_consumptions))

            def average(self) -> float:
                return statistics.fmean(map(lambda x: x.memory_consumption, self.memory_consumptions))

            def median(self) -> float:
                return statistics.median(map(lambda x: x.memory_consumption, self.memory_consumptions))

        json_file = read_json(json)
        # test = read_json(test_json)
        metrics = Metrics([])
        # test_metrics = Metrics([])
        for segment in json_file["segments"]:
            # for ref_segment, test_segment in zip(reference['segments'], test['segments']):
            column_type = segment["column_data_type"]
            column_name = segment["column_name"]
            # Only look at string columns
            if column_type != "string":
                continue
            metrics.memory_consumptions.append(
                MemoryConsumption(column_name, column_type, segment["estimated_size_in_bytes"])
            )
            # test_metrics.memory_consumptions.append(MemoryConsumption(column_name, column_type, test_segment["estimated_size_in_bytes"]))
        # output(f'For reference encoding {reference_encoding}: {{ min: {reference_metrics.min()}, max: {reference_metrics.max()}, average: {reference_metrics.average()}, median: {reference_metrics.median()} }}; ' +
        #        f'For test encoding {test_encoding}: {{ min: {test_metrics.min()}, max: {test_metrics.max()}, average: {test_metrics.average()}, median: {test_metrics.median()} }}', required_verbosity_level=2)
        return list(map(lambda x: x.memory_consumption, metrics.memory_consumptions))

    def _run(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> str:
        self._pre_run_cleanup()
        print_debug(f"Running benchmark {self.name} for encoding {encoding}.", required_verbosity_level=1)
        st_command = self._get_arguments(threading, encoding, metrics)
        print_debug(f'Command: `{" ".join(map(lambda x: enquote(x), st_command))}`', required_verbosity_level=2)
        st_output = check_output(st_command)
        print_debug(f"Output of above command: `{st_output}`", required_verbosity_level=3)
        return self._output_path(threading, encoding, metrics)

    def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
        arguments = [self._path, "-o", self._output_path(threading, encoding, metrics), "-e", encoding]
        if threading == "MT":
            arguments += ["--scheduler", "--clients", str(multiprocessing.cpu_count() // 4), "--mode=Shuffled"]
            # Multithreaded runs need longer times to be meaningful. Default to 20 minutes.
            arguments += ["-t", str(self._config.time_limit * 20)]
        else:
            arguments += ["-t", str(self._config.time_limit)]
        if metrics:
            arguments += ["--metrics", "-r", "1"]
        return arguments

    @abstractmethod
    def _pre_run_cleanup(self) -> None:
        pass

    @property
    def _path(self) -> str:
        pass

    def _output_path(self, threading: Literal["ST"] | Literal["MT"], encoding: str, metrics: bool) -> str:
        return path.join(self._config.tmp_path, f"{self.name}-{threading}-{encoding}-{metrics}.json")


class TPCHBenchmark(Benchmark):
    name = "tpch"

    def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
        return super()._get_arguments(threading, encoding, metrics) + ["-s", str(self._config.scale_factor)]

    def _pre_run_cleanup(self) -> None:
        rm_dir("tpch_cached_tables")

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, "hyriseBenchmarkTPCH")


class TPCDSBenchmark(Benchmark):
    name = "tpcds"

    def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
        # TPC-DS only supports integer scales
        return super()._get_arguments(threading, encoding, metrics) + [
            "-s",
            str(max(1, int(self._config.scale_factor))),
        ]

    def _pre_run_cleanup(self) -> None:
        rm_dir("tpcds_cached_tables")

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, "hyriseBenchmarkTPCDS")


class JoinOrderBenchmark(Benchmark):
    name = "job"

    def _pre_run_cleanup(self) -> None:
        rm_dir("imdb_data")

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, "hyriseBenchmarkJoinOrder")


class StarSchemaBenchmark(Benchmark):
    name = "ssb"

    def _pre_run_cleanup(self) -> None:
        rm_dir("imdb_data")

    @property
    def _path(self) -> str:
        return path.join(self._config.build_path, "hyriseBenchmarkStarSchema")


def parse_arguments() -> Configuration:
    global VERBOSITY_LEVEL

    def check_positive(value: any) -> int:
        ivalue = int(value)
        if ivalue <= 0:
            raise ArgumentTypeError("%s is an invalid positive int value" % value)
        return ivalue

    parser = ArgumentParser()
    parser.add_argument(
        "-o",
        "--output-directory",
        dest="output_directory",
        type=str,
        required=True,
        help="The directory where the output should be stored.",
    )
    parser.add_argument(
        "-b",
        "--build-path",
        dest="build_path",
        type=str,
        required=True,
        help="Path where the executables to benchmark are located.",
    )
    parser.add_argument(
        "-s",
        "--scale-factor",
        dest="scale_factor",
        type=float,
        required=True,
        help="The scale factor to pass to the benchmarks that support scaling. Note that this number might get rounded or ignored if necessary for a benchmark.",
    )
    parser.add_argument(
        "-p",
        "--tmp-path",
        dest="tmp_path",
        type=str,
        required=False,
        default="tmp",
        help="The directory where the benchmark result files will be stored.",
    )
    parser.add_argument(
        "-e",
        "--encoding-under-test",
        dest="encoding",
        type=str,
        required=True,
        help="The name of the encoding to compare against built-in encodings.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=check_positive,
        required=False,
        default=60,
        help="The timeout in seconds to pass to the benchmarks. Defaults to 60.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbosity_level",
        required=False,
        default=0,
        action="count",
        help="Verbosity level. Supports multiple ocurrences (like `-vvv`) to increase verbosity.",
    )
    namespace = parser.parse_args()
    if namespace.verbosity_level > 0:
        VERBOSITY_LEVEL = namespace.verbosity_level
        print_debug(f"Verbosity Mode enabled on level {VERBOSITY_LEVEL}.", required_verbosity_level=VERBOSITY_LEVEL)
    now = datetime.now()
    now_date = f"{now.year}{now.month:02d}{now.day:02d}"
    now_time = f"{now.hour}{now.minute:02d}{now.second:02d}"
    return Configuration(
        output_directory=path.join(namespace.output_directory, f"run-{now_date}-{now_time}"),
        build_path=namespace.build_path,
        tmp_path=namespace.tmp_path,
        scale_factor=namespace.scale_factor,
        encoding_under_test=namespace.encoding,
        time_limit=namespace.timeout,
        verbosity_level=namespace.verbosity_level,
    )


def scale_factor_for_benchmark(benchmark: str, scale_factor: float) -> float:
    if benchmark == "hyriseBenchmarkTPCDS":
        return max(scale_factor, 1)
    return scale_factor


def locate_benchmarks(benchmarks: list[str], config: Configuration) -> list[Benchmark]:
    benchmark_objects: list[Benchmark] = []
    for benchmark in benchmarks:
        benchmark_path = path.join(config.build_path, benchmark)
        if not path.isfile(benchmark_path):
            exit(f"Cannot locate {benchmark} at {benchmark_path}!")
        benchmark_objects.append(Benchmark.create(benchmark))
    return benchmark_objects


def main():
    global output_file
    config = parse_arguments()
    Path(config.tmp_path).mkdir(parents=True, exist_ok=True)
    Path(config.output_directory).mkdir(parents=True, exist_ok=True)
    if True:
        # with open(config.output_file, 'w+') as output_file:
        print_debug(
            f"Running benchmark comparing {config.encoding_under_test} Encoding against built-in encodings.",
            required_verbosity_level=1,
        )
        benchmarks_names = [
            "hyriseBenchmarkTPCH",
            "hyriseBenchmarkTPCDS",
            "hyriseBenchmarkJoinOrder",
            "hyriseBenchmarkStarSchema",
        ]
        benchmarks = locate_benchmarks(benchmarks_names, config)
        for benchmark in benchmarks:
            benchmark.run(config)
            benchmark.compare_metrics(config)


if __name__ == "__main__":
    main()
