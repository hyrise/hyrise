#!/usr/bin/env python3

# To prevent _tkinter.TclError: https://stackoverflow.com/a/37605654
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import os
import argparse
import multiprocessing
import subprocess
import json

from datetime import datetime


# This script executes a Hyrise benchmark multiple times over a specified range of cores, and produces a plot
# of the results. You'll have to provide the path to the benchmark executable (e.g., hyriseBenchmarkTPCH)
# as argument, as well as all parameters you want to be passed to the executable, like --scale, --chunk_size, etc.
# You'll also have to specify the number of query runs as either a fixed number for all cores (--fixed-runs),
# or a number of runs per core (--runs-per-core).
# You can also specify the cores to be benchmarked, otherwise a default range will be used.
#
# Example usage 1:
# python3 ./scripts/benchmark_multithreaded.py -e ./build-release/hyriseBenchmarkTPCH --runs 1000 -v
#
# Example usage 2:
# python3 ./scripts/benchmark_multithreaded.py -e ./build-release/hyriseBenchmarkTPCH --runs 1000 -v --scale 1 --cores 10 20 30 --queries 1,3,6,12 --clients 20


MAX_CORE_COUNT = multiprocessing.cpu_count()
DEFAULT_TPCH_QUERIES = ','.join([str(query) for query in range(1, 23)])

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Print log messages')
    parser.add_argument('-q', '--queries', action='store', type=str, metavar='Q', help='Specify the queries that will be benchmarked (comma-separated list of query IDs, e.g. --queries 1,3,18')
    parser.add_argument('-e', '--executable', action='store', type=str, metavar='E', help='benchmark executable (e.g., hyriseBenchmarkTPCH)', required=True)
    parser.add_argument('--result-dir', action='store', type=str, metavar='DIR', default='results', help='Directory where the result folder will be stored (default: \'results/\')')
    parser.add_argument('--result-name', action='store', type=str, metavar='NAME', help='Directory where the actual results will be stored (default: current datetime)')
    parser.add_argument('--clients', action='store', type=int, metavar='C', default=10, help='The number of clients that schedule queries in parallel')

    core_group = parser.add_mutually_exclusive_group()
    core_group.add_argument('-c', '--cores', action='store', type=int, metavar='C', nargs='+', help='List of cores to be used for the benchmarks')
    core_group.add_argument('--max-cores', action='store', type=int, metavar='C', help='Number of cores up to which the benchmarks will be executed')

    run_group = parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument('-r', '--runs', action='store', type=int, metavar='N', help='Fixed number of (max) runs each query is executed')
    run_group.add_argument('--runs-per-core', action='store', type=int, metavar='N', help='Number of (max) runs per core each query is executed')
    
    return parser

def verbose_print(verbose, message):
    if verbose: print('python> ' + message)

def get_core_counts(args):
    if args.cores:
        return args.cores
    max_core_count = args.max_cores if args.max_cores else MAX_CORE_COUNT
    if max_core_count < 8:
        step = -1
    elif max_core_count < 16:
        step = -int(max_core_count / 4)
    else:
        step = -int(max_core_count / 8)
    core_counts = list(range(max_core_count, 0, step))
    if core_counts[-1] != 1:
        core_counts.append(1)
    core_counts.append(0)
    return core_counts

def get_formatted_queries(args):
    queries = args.queries if args.queries else DEFAULT_TPCH_QUERIES
    return ['--queries', queries]

def is_square(n):
    return n ** 0.5 == int(n ** 0.5)

def get_subplot_row_and_column_count(num_plots):
    while not is_square(num_plots):
        num_plots += 1
    return num_plots ** 0.5

def run_benchmarks(args, hyrise_args, core_counts, result_dir):
    benchmark_run = 0
    for core_count in core_counts:
        benchmark_run += 1
        verbose_print(args.verbose, 'Starting benchmark ' + str(benchmark_run) + ' of ' + str(len(core_counts)) + ' ...')

        if core_count == 0:
            use_scheduler = 'false'
            number_of_runs = args.runs_per_core if args.runs_per_core else args.runs
        else:
            use_scheduler = 'true'
            number_of_runs = args.runs_per_core * core_count if args.runs_per_core else args.runs

        file_name = str(core_count) + '-cores.json'
        result_file = os.path.join(result_dir, file_name)
        execution_command = [
            args.executable,
            '--output', result_file,
            '--runs', str(number_of_runs),
            '--scheduler=' + use_scheduler,
            '--cores', str(core_count),
            '--clients', str(args.clients),
        ]
        if args.verbose:
            execution_command.append('--verbose')
        if hyrise_args:
            execution_command += hyrise_args
        execution_command += get_formatted_queries(args)
        verbose_print(args.verbose, 'Executing command: ' + subprocess.list2cmdline(execution_command) + '\n')
        subprocess.run(execution_command)

def benchmark(args, hyrise_args):
    result_name = args.result_name if args.result_name else datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    result_dir = os.path.join(args.result_dir, result_name)
        
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    core_counts = get_core_counts(args)
    verbose_print(args.verbose, 'Executing benchmarks with the following numbers of cores: ' + str(core_counts))

    # Make sure our machine has the number of cores we are trying to benchmark with
    for core_count in core_counts:
        assert core_count <= MAX_CORE_COUNT, 'Can\'t use more cores than we have available'

    run_benchmarks(args, hyrise_args, core_counts, result_dir)

    verbose_print(args.verbose, 'Benchmarks complete!')
    verbose_print(args.verbose, 'Saved JSON results to: ' + result_dir)

    return result_dir

def plot(args, result_dir):
    verbose_print(args.verbose, 'Plotting results from: ' + result_dir)

    utilized_cores_per_numa_node = []
    max_cores = 0

    results = {}
    for _, _, files in os.walk(result_dir):
        json_files = [f for f in files if f.split('.')[-1] == 'json']
        num_cores_from_filename = lambda filename: int(filename.split('-')[0])
        # Add the results in sorted order (low core count -> high core count) for plotting later
        for file in sorted(json_files, key=num_cores_from_filename):
            with open(os.path.join(result_dir, file), 'r') as json_file:
                data = json.load(json_file)
            cores = data['context']['cores']
            if cores > max_cores:
                max_cores = cores
                utilized_cores_per_numa_node = data['context']['utilized_cores_per_numa_node']
            for benchmark in data['benchmarks']:
                name = benchmark['name']
                items_per_second = benchmark['items_per_second']
                if not name in results:
                    results[name] = {'cores': [], 'items_per_second': []}
                if cores == 0:
                    results[name]['singlethreaded'] = items_per_second
                else:
                    results[name]['cores'].append(cores)
                    results[name]['items_per_second'].append(items_per_second)

    numa_borders = [sum(utilized_cores_per_numa_node[:x]) for x in range(len(utilized_cores_per_numa_node))][1:]

    num_plots = len(results)
    n_rows_and_cols = get_subplot_row_and_column_count(num_plots)
    plot_pos = 0

    plot_baseline = False
    fig = plt.figure(figsize=(n_rows_and_cols*3, n_rows_and_cols*2))
    for name, data in results.items():
        plot_pos += 1
        ax = fig.add_subplot(n_rows_and_cols, n_rows_and_cols, plot_pos)
        ax.set_title(name)

        multithreaded_plot = ax.plot(data['cores'], data['items_per_second'], label='multithreaded', marker='.')
        if 'singlethreaded' in data:
            plot_baseline = True
            singlethreaded_plot = ax.axhline(data['singlethreaded'], color=multithreaded_plot[0].get_color(), linestyle='dashed', linewidth=1.0, label='singlethreaded')
        ax.set_ylim(ymin=0)
        ax.set_xlim(xmin=0, xmax=max_cores)

        for numa_border in numa_borders:
            ax.axvline(numa_border, color='gray', linestyle='dashed', linewidth=1.0)

    if plot_baseline:
        plt.figlegend((multithreaded_plot[0], singlethreaded_plot), ('multithreaded', 'singlethreaded'), 'lower right')

    # This should prevent axes from different plots to overlap etc
    plt.tight_layout()

    # Add big axis descriptions for all plots
    ax_legend = fig.add_subplot(111, frameon=False)
    plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)
    ax_legend.set_xlabel('Utilized cores', labelpad=10)
    ax_legend.set_ylabel('Throughput (queries / sec)', labelpad=20)

    # Save as png (rasterized) and pdf (vectorized)
    for extension in ['png', 'pdf']:
        result_plot_file = os.path.join(result_dir, 'result_plots.' + extension)
        plt.savefig(result_plot_file, bbox_inches='tight')
        # Always print out where the plot was saved, independent of verbosity
        verbose_print(True, 'Plot saved as: ' + result_plot_file)

if __name__ == "__main__":
    parser = get_parser()
    args, hyrise_args = parser.parse_known_args()

    for arg in hyrise_args:
        if arg.startswith('--scheduler'):
            parser.print_help()
            print('\nerror: The \'--scheduler\' argument will be set by this script and not by the user')
            exit(1)

    result_dir = benchmark(args, hyrise_args)
    plot(args, result_dir)
