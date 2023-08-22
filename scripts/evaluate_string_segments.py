#!/usr/bin/env python3
import argparse
import json
import shutil
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentTypeError, BooleanOptionalAction
from dataclasses import dataclass
import multiprocessing
import os
from pathlib import Path
import statistics
from subprocess import check_output, CalledProcessError
import sys
from datetime import datetime
from typing import Any, Literal, Mapping

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

DEFAULT_ENCODINGS = ["Unencoded", "Dictionary", "RunLength", "FixedStringDictionary", "LZ4"]


def print_error(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def rm_dir(path: str) -> None:
    dirpath = Path(path)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


def read_json(path: str) -> dict:
    with open(path) as file:
        return json.load(file)


def flatten(x: list[list], /) -> list:
    return [item for sublist in x for item in sublist]


class DictConvertible(ABC):
    @abstractmethod
    def as_dict(self) -> dict:
        ...


@dataclass(frozen=True, slots=True)
class Runtime(DictConvertible):
    benchmark_name: str
    durations: list[float]

    def as_dict(self) -> dict:
        return {
            "benchmark_name": self.benchmark_name,
            "durations": self.durations,
        }

    def min(self) -> float:
        return min(self.durations)

    def max(self) -> float:
        return max(self.durations)

    def average(self) -> float:
        return statistics.fmean(self.durations)

    def median(self) -> float:
        return statistics.median(self.durations)


@dataclass(slots=True)
class Runtimes(DictConvertible):
    runtimes: list[Runtime]
    encoding: str
    benchmark_name: str
    threading: str

    def as_dict(self) -> dict:
        return {"runtimes": list(runtime.as_dict() for runtime in self.runtimes)}

    def min(self) -> float:
        return min(map(lambda x: x.median(), self.runtimes))

    def max(self) -> float:
        return max(map(lambda x: x.median(), self.runtimes))

    def average(self) -> float:
        return statistics.fmean(map(lambda x: x.median(), self.runtimes))

    def median(self) -> float:
        return statistics.median(map(lambda x: x.median(), self.runtimes))

    @classmethod
    def from_json(cls, json_path: str) -> "Runtimes":
        json_data = read_json(json_path)
        runtimes = cls(
            [], f"{json_data['branch']}-{json_data['encoding']}", json_data["benchmark_name"], json_data["threading"]
        )
        json_data = json_data["benchmark"]
        for benchmark in json_data["benchmarks"]:
            name = benchmark["name"]
            durations: list[float] = [run["duration"] for run in benchmark["successful_runs"]]
            runtime = Runtime(name, durations)
            runtimes.runtimes.append(runtime)
        return runtimes


@dataclass(frozen=True, slots=True)
class MemoryConsumption(DictConvertible):
    column_name: str
    column_type: str
    memory_consumption: int

    def as_dict(self) -> dict:
        return {
            "column_name": self.column_name,
            "column_type": self.column_type,
            "memory_consumption": self.memory_consumption,
        }


@dataclass(slots=True)
class Metrics(DictConvertible):
    memory_consumptions: list[MemoryConsumption]
    encoding: str
    benchmark_name: str

    def as_dict(self) -> dict:
        return {"memory_consumption": list(consumption.as_dict() for consumption in self.memory_consumptions)}

    def _as_generator(self, *, only_string_columns: bool):
        return (
            consumption
            for consumption in self.memory_consumptions
            if not only_string_columns or consumption.column_type == "string"
        )

    def min(self, *, only_string_columns: bool) -> int:
        return min(map(lambda x: x.memory_consumption, self._as_generator(only_string_columns=only_string_columns)))

    def max(self, *, only_string_columns: bool) -> int:
        return max(map(lambda x: x.memory_consumption, self._as_generator(only_string_columns=only_string_columns)))

    def average(self, *, only_string_columns: bool) -> float:
        return statistics.fmean(
            map(lambda x: x.memory_consumption, self._as_generator(only_string_columns=only_string_columns))
        )

    def median(self, *, only_string_columns: bool) -> float:
        return statistics.median(
            map(lambda x: x.memory_consumption, self._as_generator(only_string_columns=only_string_columns))
        )

    def sum(self, *, only_string_columns: bool) -> int:
        return sum(map(lambda x: x.memory_consumption, self._as_generator(only_string_columns=only_string_columns)))

    @classmethod
    def from_json(cls, json_path: str) -> "Metrics":
        json_data = read_json(json_path)
        metrics = cls([], f"{json_data['branch']}-{json_data['encoding']}", json_data["benchmark_name"])
        json_data = json_data["benchmark"]
        for segment in json_data["segments"]:
            # Only consider moment == "init".
            if segment["moment"] != "init":
                continue
            column_type = segment["column_data_type"]
            column_name = segment["column_name"]
            estimated_size = segment["estimated_size_in_bytes"]
            metrics.memory_consumptions.append(
                MemoryConsumption(column_name, column_type, estimated_size)
            )
        return metrics


def clean_encoding_name(encoding: str) -> str:
    return encoding_without_branch\
        if (encoding_without_branch := encoding.split('-')[-1]) in DEFAULT_ENCODINGS\
        or encoding_without_branch.split(' ')[0] in DEFAULT_ENCODINGS\
        else encoding


def plot(results: dict[str, list], *, title: str, yaxis: str, path: str, figsize: tuple[int, int] = (15, 10)) -> None:
    f, axis = plt.subplots(1, 1, figsize=figsize)
    # The transposing of the orientation is done to allow for empty cells.
    data = pd.DataFrame.from_dict(results, orient="index")
    data = data.transpose()
    data = data.rename(clean_encoding_name, axis='columns')
    if data.empty:
        print_error("Data Frame is empty; no result data to show!")
        return
    data.plot(
        kind="box",
        ax=axis,
        title=title,
        xlabel="Encodings",
        ylabel=f"{yaxis} (Logarithmic Scale)",
        logy=True,
    )
    axis.set_xticklabels(axis.get_xticklabels(), rotation=-45)
    f.tight_layout()
    f.savefig(path)


def refine_stats(
    stats: dict[str, tuple[Mapping[Literal["ST", "MT"], Mapping[str, Runtimes]], Mapping[str, Metrics]]]
) -> dict:
    result = {"ENCODING": [], "RUNTIME": [], "MODE": [], "SIZE": [], "BENCHMARK": []}
    for benchmark_name, benchmark_results in stats.items():
        for threading, runtimes in benchmark_results[0].items():
            for encoding, runtime_wrapper in runtimes.items():
                runtime = runtime_wrapper.average() / 1e9  # Given in nanoseconds, return seconds
                metrics = benchmark_results[1][encoding]
                size = metrics.sum(only_string_columns=True) / 1e6  # Given in Bytes, return MB
                result["SIZE"].append(size)
                result["RUNTIME"].append(runtime)
                result["MODE"].append(threading)
                cleaned_encoding_name = clean_encoding_name(encoding)
                result["ENCODING"].append(cleaned_encoding_name)
                result["BENCHMARK"].append(benchmark_name)
    return result


def plot_stats(
    stats: dict[str, tuple[Mapping[Literal["ST", "MT"], Mapping[str, Runtimes]], Mapping[str, Metrics]]], *, path: str,
        sharex: bool, sharey: bool
) -> None:
    data = pd.DataFrame.from_dict(refine_stats(stats))
    data = data.sort_values(['BENCHMARK', 'ENCODING'])
    g = sns.FacetGrid(data, col="BENCHMARK", row="MODE", hue="ENCODING", sharex=sharex, sharey=sharey)
    g.map(sns.scatterplot, "SIZE", "RUNTIME")
    g.set(xscale="log")
    g.add_legend()
    g.set_axis_labels("Memory Consumption [Sum MB] (Log)", "Duration [Avg Seconds]")
    g.tight_layout()
    g.savefig(path)


def plot_query_timings(stats: list[Runtimes], *, benchmark_name: str, path: str,
                       figsize: tuple[int, int] = (30, 10)) -> None:
    stats = [stat for stat in stats if stat.threading == 'ST']
    grouped: dict[str, list[Runtimes]] = Evaluation.group_by(stats, "encoding")
    stats_grouped_by_encoding = {
        encoding: {
            runtime.benchmark_name: runtime.median() / 1e9  # Provided in ns, wanted s
            for runtime
            in flatten(list(map(lambda x: x.runtimes, runtimes)))
        }
        for encoding, runtimes
        in grouped.items()
    }
    data = pd.DataFrame.from_dict(stats_grouped_by_encoding)
    data = data.rename(clean_encoding_name, axis='columns')
    data = data.reindex(sorted(data.columns), axis=1)
    f, axis = plt.subplots(1, 1, figsize=figsize)
    if data.empty:
        print_error("Data Frame is empty; no result data to show!")
        return
    data.plot(
        kind="bar",
        ax=axis,
        title=f"Query times for benchmark {benchmark_name} (ST)",
        xlabel="Queries",
        ylabel=f"Median Runtime (seconds) across all runs of the query (Logarithmic Scale)",
        logy=True
    )
    f.tight_layout()
    f.savefig(path)


class Benchmarking:
    @dataclass(frozen=True)
    class Config:
        benchmarks: list[str]
        encodings: list[str]
        build_path: str
        threading: Literal["ST", "MT", "both"]
        time_limit: int
        scale_factor: float
        tmp_path: str
        metrics: bool

        @classmethod
        def from_namespace(cls, namespace: argparse.Namespace) -> "Benchmarking.Config":
            encodings = flatten(namespace.encodings)
            if namespace.default_encodings:
                encodings.extend(DEFAULT_ENCODINGS)
            if len(encodings) == 0:
                exit("No encodings to test")
            return cls(
                benchmarks=flatten(namespace.benchmarks),
                encodings=encodings,
                build_path=namespace.build_path,
                threading=namespace.threading,
                time_limit=namespace.time_limit,
                scale_factor=namespace.scale_factor,
                tmp_path=namespace.tmp_path,
                metrics=namespace.metrics,
            )

    @staticmethod
    def register_arguments(parser: ArgumentParser) -> ArgumentParser:
        def check_positive(value) -> int:
            ivalue = int(value)
            if ivalue <= 0:
                raise ArgumentTypeError("%s is an invalid positive int value" % value)
            return ivalue

        parser.add_argument(
            "-b",
            "--benchmark",
            action="append",
            dest="benchmarks",
            nargs="*",
            help="A benchmark (e.g. hyriseBenchmarkTPCH) to run.",
        )
        parser.add_argument(
            "-e",
            "--encoding",
            action="append",
            dest="encodings",
            nargs="*",
            default=[],
            help="An encoding that should be tested.",
        )
        parser.add_argument(
            "-p",
            "--build-path",
            dest="build_path",
            type=str,
            required=True,
            help="Path where the executables to benchmark are located.",
        )
        parser.add_argument(
            "-t",
            "--threading",
            dest="threading",
            type=str,
            required=False,
            default="both",
            help="Which threading should be executed. Variants: ST, MT, both",
        )
        parser.add_argument(
            "-l",
            "--timeout",
            dest="time_limit",
            type=check_positive,
            required=False,
            default=60,
            help="The timeout in seconds to pass to the benchmarks. Defaults to 60.",
        )
        parser.add_argument(
            "-s",
            "--scale-factor",
            dest="scale_factor",
            type=float,
            required=True,
            help="The scale factor to pass to the benchmarks that support scaling. Note that this number might get "
            "rounded or ignored if necessary for a benchmark.",
        )
        parser.add_argument(
            "--tmp-path",
            dest="tmp_path",
            type=str,
            required=False,
            default="tmp",
            help="The directory where the benchmark result files will be stored.",
        )
        parser.add_argument(
            "-d",
            "--default-encodings",
            dest="default_encodings",
            action=BooleanOptionalAction,
            default=False,
            help="Whether to run benchmarks for all default encodings.",
        )
        parser.add_argument(
            "-m",
            "--metrics",
            dest="metrics",
            action=BooleanOptionalAction,
            default=False,
            help="Whether to run metrics benchmarks or timing benchmarks.",
        )
        return parser

    @staticmethod
    def run(config: Config) -> list[Path]:
        """
        Runs all benchmarks as outlined in the passed-in config.

        Returns a list of paths of output files, one for each benchmark.
        """

        @dataclass(frozen=True)
        class BenchmarkConfig(ABC):
            encoding: str
            threading: Literal["ST", "MT"]
            time_limit: int
            scale_factor: float
            metrics: bool

        class BenchmarkRunner:
            name = "Benchmark"

            def __init__(self, path: str, tmp_path: str):
                self._config: BenchmarkConfig | None = None
                self._path = path
                self._tmp_path = tmp_path

            @staticmethod
            def create(name: str, *args: Any, **kwargs: Any) -> "BenchmarkRunner":
                classes = {
                    "hyriseBenchmarkTPCH": TpcHBenchmarkRunner,
                    "hyriseBenchmarkTPCDS": TpcDsBenchmarkRunner,
                    "hyriseBenchmarkJoinOrder": JobBenchmarkRunner,
                    "hyriseBenchmarkStarSchema": SsbBenchmarkRunner,
                }
                return classes[name](*args, **kwargs)

            def _write_encoding_config_file(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> str:
                """
                Create a config file as depicted in the `--full_help` of the benchmarks and return its path.
                """
                config_path = os.path.join(self._tmp_path, "config",
                                           f"{self.name}-{threading}-{encoding}-{metrics}.json")
                config_contents = {"default": {"encoding": "Dictionary"}, "type": {"string": {"encoding": encoding}}}
                with open(config_path, mode="w") as config_file:
                    json.dump(config_contents, config_file)
                return config_path

            def run(self, benchmark_config: BenchmarkConfig) -> Path:
                self._config = benchmark_config
                return Path(self._run(self._config.threading, self._config.encoding, self._config.metrics))

            def _run(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> str:
                self._pre_run_cleanup()
                st_command = self._get_arguments(threading, encoding, metrics)
                check_output(st_command)
                return self._output_path(threading, encoding, metrics)

            def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
                encoding_config_path = self._write_encoding_config_file(threading, encoding, metrics)
                arguments = [
                    self._path,
                    "-o",
                    self._output_path(threading, encoding, metrics),
                    "-e",
                    encoding_config_path,
                ]
                if threading == "MT":
                    arguments += ["--scheduler", "--clients", str(multiprocessing.cpu_count() // 4), "--mode=Shuffled"]
                    # Multithreaded runs need longer times to be meaningful. Default to 20 minutes.
                    arguments += ["-t", str(self._config.time_limit * 20)]
                else:
                    arguments += ["-t", str(self._config.time_limit)]
                if metrics:
                    arguments += ["--metrics", "-r", "1"]
                return arguments

            def _output_path(self, threading: Literal["ST"] | Literal["MT"], encoding: str, metrics: bool) -> str:
                try:
                    git_commit = check_output(["git", "rev-parse", "HEAD"])
                except CalledProcessError:
                    git_commit = b""
                return os.path.join(
                    self._tmp_path,
                    f'{git_commit.decode("utf-8").strip()}-{self.name}-{threading}-{encoding}-{metrics}.json',
                )

            @abstractmethod
            def _pre_run_cleanup(self):
                ...

        class TpcHBenchmarkRunner(BenchmarkRunner):
            name = "TPC-H"

            def _pre_run_cleanup(self) -> None:
                rm_dir("tpch_cached_tables")

            def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
                return super()._get_arguments(threading, encoding, metrics) + ["-s", str(self._config.scale_factor)]

        class TpcDsBenchmarkRunner(BenchmarkRunner):
            name = "TPC-DS"

            def _pre_run_cleanup(self) -> None:
                rm_dir("tpcds_cached_tables")

            def _get_arguments(self, threading: Literal["ST", "MT"], encoding: str, metrics: bool) -> list[str]:
                # TPC-DS only supports integer scales.
                return super()._get_arguments(threading, encoding, metrics) + [
                    "-s",
                    str(max(1, int(self._config.scale_factor))),
                ]

        class JobBenchmarkRunner(BenchmarkRunner):
            name = "JOB"

            def _pre_run_cleanup(self) -> None:
                rm_dir("imdb_data")

        class SsbBenchmarkRunner(BenchmarkRunner):
            name = "SSB"

            def _pre_run_cleanup(self) -> None:
                rm_dir("imdb_data")

        def locate_benchmarks(benchmark_names: list[str], build_path: str, tmp_path: str) -> list[BenchmarkRunner]:
            benchmark_objects: list[BenchmarkRunner] = []
            for benchmark_name in benchmark_names:
                benchmark_path = os.path.join(build_path, benchmark_name)
                if not os.path.isfile(benchmark_path):
                    exit(f"Cannot locate {benchmark_name} at {benchmark_path}!")
                benchmark_objects.append(BenchmarkRunner.create(benchmark_name, benchmark_path, tmp_path))
            return benchmark_objects

        (Path(config.tmp_path) / Path("config")).mkdir(parents=True, exist_ok=True)
        benchmarks = locate_benchmarks(config.benchmarks, config.build_path, config.tmp_path)
        result_paths: list[Path] = []
        threading_modes: list[Literal["ST", "MT"]] = ["ST", "MT"] if config.threading == "both" else [config.threading]
        if config.metrics:
            threading_modes = ["ST"]
        total_runs = len(benchmarks) * len(config.encodings) * len(threading_modes)
        current_benchmark = 0
        for benchmark in benchmarks:
            for encoding_to_benchmark in config.encodings:
                for threading_mode in threading_modes:
                    current_benchmark += 1
                    print(f"Running benchmark {current_benchmark}/{total_runs}")
                    print(f"\tCurrently running {benchmark.name} with {encoding_to_benchmark=} and {threading_mode=}")
                    run_config = BenchmarkConfig(
                        encoding=encoding_to_benchmark,
                        threading=threading_mode,
                        time_limit=config.time_limit,
                        scale_factor=config.scale_factor,
                        metrics=config.metrics,
                    )
                    result_path = benchmark.run(run_config)
                    # Add encoding name to result json.
                    with open(result_path, mode="r") as result_json:
                        result_json_content = json.load(result_json)
                    try:
                        git_branch = check_output(["git", "branch", "--show-current"])
                    except CalledProcessError:
                        git_branch = b""
                    result_with_encoding = {
                        "benchmark": result_json_content,
                        "encoding": encoding_to_benchmark,
                        "benchmark_name": benchmark.name,
                        "threading": threading_mode,
                        "branch": git_branch.decode("utf-8").strip(),
                    }
                    with open(result_path, mode="w") as result_json:
                        json.dump(result_with_encoding, result_json)

                    result_paths.append(result_path)
        return result_paths


class Evaluation:
    @dataclass(frozen=True)
    class Config:
        timing_benchmark_files: list[str]
        metric_benchmark_files: list[str]
        ignore_encodings: list[str]
        output_directory: str
        sharex: bool
        sharey: bool
        only_comparison: bool

        @classmethod
        def from_namespace(cls, namespace: argparse.Namespace) -> "Evaluation.Config":
            now = datetime.now()
            now_date = f"{now.year}{now.month:02d}{now.day:02d}"
            now_time = f"{now.hour}{now.minute:02d}{now.second:02d}"
            output_directory = os.path.join(namespace.output_directory, f"run-{now_date}-{now_time}")

            return cls(
                timing_benchmark_files=flatten(namespace.timing_benchmark_files),
                metric_benchmark_files=flatten(namespace.metric_benchmark_files),
                ignore_encodings=flatten(namespace.ignore_encodings),
                output_directory=output_directory,
                sharex=namespace.sharex,
                sharey=namespace.sharey,
                only_comparison=namespace.only_comparison
            )

    @staticmethod
    def register_arguments(parser: ArgumentParser) -> ArgumentParser:
        parser.add_argument(
            "-t",
            "--timing-benchmark-files",
            action="append",
            dest="timing_benchmark_files",
            required=True,
            nargs="+",
            help="All timing benchmark files to evaluate.",
        )
        parser.add_argument(
            "-m",
            "--metric-benchmark-files",
            action="append",
            dest="metric_benchmark_files",
            required=True,
            nargs="+",
            help="All timing benchmark files to evaluate.",
        )
        parser.add_argument(
            '-i',
            '--ignore',
            action='append',
            dest='ignore_encodings',
            required=False,
            default=[],
            nargs='+',
            help='Encodings to ignore despite being present in the provided files.'
        )
        parser.add_argument(
            '-o',
            '--output-directory',
            dest='output_directory',
            type=str,
            required=True,
            help="The directory where the output should be stored.",
        )
        parser.add_argument(
            '--share-x',
            dest='sharex',
            action=BooleanOptionalAction,
            default=True,
            help='Whether to share the x axis of the comparison plot. Defaults to True.'
        )
        parser.add_argument(
            '--share-y',
            dest='sharey',
            action=BooleanOptionalAction,
            default=True,
            help='Whether to share the y axis of the comparison plot. Defaults to True.'
        )
        parser.add_argument(
            '--only-comparison',
            dest='only_comparison',
            action=BooleanOptionalAction,
            default=False,
            help='Whether to only create the comparison plot.'
        )
        return parser

    @staticmethod
    def is_excluded(encoding: str, config: Config) -> bool:
        return encoding.split('-')[-1] in config.ignore_encodings

    @staticmethod
    def run(config: Config) -> list[Path]:
        Path(config.output_directory).mkdir(parents=True, exist_ok=True)
        stats: dict[str, tuple[Mapping[Literal["ST", "MT"], Mapping[str, Runtimes]], Mapping[str, Metrics]]] = {}
        metric_comparisons, metric_paths = Evaluation._compare_metrics(config)
        timing_comparisons, timing_paths = Evaluation._compare_timing_benchmarks(config)
        metric_stats: dict[str, Mapping[str, Metrics]] = {}
        for benchmark_name, metrics in metric_comparisons.items():
            metric_stats[benchmark_name] = metrics
        for benchmark_name, runtimes in timing_comparisons.items():
            metrics = metric_stats[benchmark_name]
            stats[benchmark_name] = runtimes, metrics
        plot_path = os.path.join(config.output_directory, "comparison.svg")
        plot_stats(stats, path=plot_path, sharex=config.sharex, sharey=config.sharey)
        paths = metric_paths
        paths.extend(timing_paths)
        paths.append(Path(plot_path))
        return paths

    @staticmethod
    # Source: https://stackoverflow.com/a/5695268
    def group_by(array: list, key: str) -> dict[Any, list]:
        values = set(map(lambda x: x.__getattribute__(key), array))
        return {value: [y for y in array if y.__getattribute__(key) == value] for value in values}

    # Returns a tuple of a dictionary of the metrics grouped by benchmark and encoding with
    # path names of the corresponding plot file and raw data file.
    @staticmethod
    def _compare_metrics(config: Config) -> tuple[dict[str, dict[str, Metrics]], list[Path]]:
        paths: list[Path] = []
        result_jsons = config.metric_benchmark_files
        metrics_list = [
            metric
            for metric
            in
            [
                Metrics.from_json(result_json)
                for result_json
                in result_jsons
            ]
            if not Evaluation.is_excluded(metric.encoding, config)
        ]
        metrics_grouped_by_benchmark: dict[str, list[Metrics]] = Evaluation.group_by(metrics_list, "benchmark_name")
        metrics_grouped_by_benchmark_and_encoding_list: dict[str, dict[str, list[Metrics]]] = {
            benchmark: Evaluation.group_by(metrics_by_benchmark, "encoding")
            for benchmark, metrics_by_benchmark in metrics_grouped_by_benchmark.items()
        }
        # Ensure that every benchmark-encoding combination only has one entry.
        for benchmark_group in metrics_grouped_by_benchmark_and_encoding_list.values():
            for encoding_group in benchmark_group.values():
                if len(encoding_group) > 1:
                    raise RuntimeError(f'Too many encoding groups: {len(encoding_group)}')
        metrics_grouped_by_benchmark_and_encoding: dict[str, dict[str, Metrics]] = {
            benchmark: {encoding: entries[0] for encoding, entries in group.items()}
            for benchmark, group in metrics_grouped_by_benchmark_and_encoding_list.items()
        }
        if not config.only_comparison:
            for benchmark_name, metrics_by_benchmark in metrics_grouped_by_benchmark_and_encoding.items():
                metrics: dict[str, Metrics] = {encoding: metric for encoding, metric in metrics_by_benchmark.items()}
                plot_path = os.path.join(config.output_directory, "metrics", "plots", f"{benchmark_name}.svg")
                Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
                metrics_to_plot: dict[str, list[int]] = {}
                for encoding, metric in metrics.items():
                    metrics_to_plot[f"{encoding} (String)"] = list(
                        map(
                            lambda x: x.memory_consumption,
                            filter(lambda x: x.column_type == "string", metric.memory_consumptions),
                        )
                    )
                    metrics_to_plot[f"{encoding} (All)"] = list(
                        map(lambda x: x.memory_consumption, metric.memory_consumptions)
                    )
                plot(
                    metrics_to_plot,
                    title=f"Sizes for {benchmark_name}",
                    yaxis="Size of Segments",
                    path=plot_path,
                    figsize=(22, 10),
                )
                paths.append(Path(plot_path))
                # Dumps raw data.
                raw_file_path = os.path.join(config.output_directory, "metrics", "raw", f"{benchmark_name}.json")
                Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
                with open(raw_file_path, "w") as f:
                    raw_metrics = {encoding: metric.as_dict() for encoding, metric in metrics.items()}
                    json.dump(raw_metrics, f)
                paths.append(Path(raw_file_path))
        return metrics_grouped_by_benchmark_and_encoding, paths

    @staticmethod
    def _compare_timing_benchmarks(
        config: Config,
    ) -> tuple[dict[str, dict[Literal["ST", "MT"], dict[str, Runtimes]]], list[Path]]:
        paths: list[Path] = []
        result_jsons = config.timing_benchmark_files
        timings_list = [
            timing
            for timing
            in
            [
                Runtimes.from_json(result_json)
                for result_json
                in result_jsons
            ]
            if not Evaluation.is_excluded(timing.encoding, config)
        ]
        timings_grouped_by_benchmark: dict[str, list[Runtimes]] = Evaluation.group_by(timings_list, "benchmark_name")
        for benchmark_name, benchmark_group in timings_grouped_by_benchmark.items():
            plot_path = os.path.join(config.output_directory, "runtime", "plots", f"{benchmark_name}-queries.svg")
            Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
            plot_query_timings(benchmark_group, benchmark_name=benchmark_name, path=plot_path)
            paths.append(Path(plot_path))
        timings_grouped_by_benchmark_and_encoding_list: dict[str, dict[str, list[Runtimes]]] = {
            benchmark: Evaluation.group_by(timings_by_benchmark, "encoding")
            for benchmark, timings_by_benchmark in timings_grouped_by_benchmark.items()
        }
        # Ensures that every benchmark-encoding combination only has two entries, one per threading.
        for benchmark_group in timings_grouped_by_benchmark_and_encoding_list.values():
            for encoding_group in benchmark_group.values():
                if len(encoding_group) > 2:
                    exit(len(encoding_group))
        timings_grouped_by_benchmark_and_encoding: dict[str, dict[Literal["ST", "MT"], dict[str, Runtimes]]] = {
            benchmark: {
                "ST": {
                    encoding: next(filter(lambda x: x.threading == "ST", entries))
                    for encoding, entries in group.items()
                },
                "MT": {
                    encoding: next(filter(lambda x: x.threading == "MT", entries))
                    for encoding, entries in group.items()
                },
            }
            for benchmark, group in timings_grouped_by_benchmark_and_encoding_list.items()
        }
        if not config.only_comparison:
            threading: Literal["ST", "MT"]
            for threading in ["ST", "MT"]:
                for name, timing_group in timings_grouped_by_benchmark_and_encoding.items():
                    times = timing_group[threading]
                    plot_path = os.path.join(config.output_directory, "runtime", "plots", f"{name}-{threading}.svg")
                    Path(plot_path).parent.mkdir(parents=True, exist_ok=True)
                    times_to_plot = {
                        encoding: list(map(lambda x: x.median(), runtimes.runtimes))
                        for encoding, runtimes in times.items()
                    }
                    plot(
                        times_to_plot,
                        title=f"Median duration for {name}",
                        yaxis="Median runtime (in ns) across benchmark tests",
                        path=plot_path,
                    )
                    paths.append(Path(plot_path))
                    # Dumps raw data.
                    raw_file_path = os.path.join(config.output_directory, "runtime", "raw", f"{name}-{threading}.json")
                    Path(raw_file_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(raw_file_path, "w") as f:
                        raw_times = {encoding: runtimes.as_dict() for encoding, runtimes in times.items()}
                        json.dump(raw_times, f)
                    paths.append(Path(raw_file_path))
        return timings_grouped_by_benchmark_and_encoding, paths


def create_argument_parser() -> ArgumentParser:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(help="Mode of the script", dest="command")
    benchmark_parser = subparsers.add_parser("benchmark", help="Run benchmarks for Hyrise")
    Benchmarking.register_arguments(benchmark_parser)
    evaluation_parser = subparsers.add_parser("evaluate", help="Evaluate benchmark results")
    Evaluation.register_arguments(evaluation_parser)
    return parser


def main():
    parser = create_argument_parser()
    namespace = parser.parse_args()
    command = namespace.command
    match command:
        case "benchmark":
            config = Benchmarking.Config.from_namespace(namespace)
            files = Benchmarking.run(config)
            print(f"Resulting files: {files}")
        case "evaluate":
            config = Evaluation.Config.from_namespace(namespace)
            files = Evaluation.run(config)
            print(f"Resulting files: {files}")
        case _:
            print_error(f"Could not find command '{command}'!")
            exit(1)


if __name__ == "__main__":
    main()
