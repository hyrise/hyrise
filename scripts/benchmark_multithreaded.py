import os
import argparse
import multiprocessing
import subprocess

from datetime import datetime


BENCHMARK_EXECUTABLE = 'hyriseBenchmarkTPCH'
DEFAULT_TPCH_QUERIES = [query for query in range(23) if query != 15] # Exclude query 15 which is not supported in our multithreaded benchmarks

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cores', action='store', type=int, metavar='N', help='Number of cores this machine has available')
    parser.add_argument('-s', '--scale', action='store', type=float, metavar='S', default=0.1, help='TPC-H scale factor (default: 0.1)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print log messages')
    parser.add_argument('-q', '--queries', action='store', type=int, nargs='+', help='Specify the TPC-H queries that will be benchmarked')
    parser.add_argument('--chunk-size', action='store', type=int, metavar='S', help='Specify maximum chunk size (default: Maximum available)')
    parser.add_argument('--result-dir', action='store', type=str, metavar='DIR', default='results', help='Directory where the results will be stored (default: \'results/\')')
    parser.add_argument('--build-dir', action='store', type=str, metavar='DIR', required=True, help='Directory that contains the hyriseBenchmarkTPCH executable')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--fixed-runs', action='store', type=int, metavar='N', help='Fixed number of runs each query is executed')
    group.add_argument('--runs-per-core', action='store', type=int, metavar='N', help='Number of runs per core each query is executed')
    
    return parser.parse_args()

def verbose_print(verbose, message):
    if verbose: print('python> ' + message)

def get_core_counts(args):
    max_core_count = args.cores if args.cores else multiprocessing.cpu_count()
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
    formatted_queries = []
    for query in queries:
        formatted_queries += ['-q', str(query)]
    return formatted_queries

def run_benchmarks(args, core_counts, executable, result_dir):
    benchmark_run = 0
    for core_count in core_counts:
        benchmark_run += 1
        verbose_print(args.verbose, 'Starting benchmark run ' + str(benchmark_run) + ' of ' + str(len(core_counts)) + ' ...')

        if core_count == 0:
            use_scheduler = 'false'
            number_of_runs = args.runs_per_core if args.runs_per_core else args.fixed_runs
        else:
            use_scheduler = 'true'
            number_of_runs = args.runs_per_core * core_count if args.runs_per_core else args.fixed_runs

        file_name = str(core_count) + '-cores.json'
        result_file = os.path.join(result_dir, file_name)
        execution_command = [
            executable,
            '--parallel',
            '--output', result_file,
            '--scale', str(args.scale),
            '--runs', str(number_of_runs),
            '--scheduler', use_scheduler,
            '--cores', str(core_count),
        ]
        if args.chunk_size:
            execution_command += ['--chunk_size', str(args.chunk_size)]
        if args.verbose:
            execution_command.append('--verbose')
        execution_command += get_formatted_queries(args)
        verbose_print(args.verbose, 'Executing command: ' + subprocess.list2cmdline(execution_command) + '\n')
        subprocess.run(execution_command)

def benchmark(args):
    core_counts = get_core_counts(args)

    executable = os.path.join(args.build_dir, BENCHMARK_EXECUTABLE)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    result_dir = os.path.join(args.result_dir, timestamp)
        
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    verbose_print(args.verbose, 'Executing benchmarks with the following numbers of cores: ' + str(core_counts))

    run_benchmarks(args, core_counts, executable, result_dir)

    verbose_print(args.verbose, 'Benchmarks complete!')
    verbose_print(args.verbose, 'Saved results to: ' + result_dir)

def plot(args):
    pass

if __name__ == "__main__":
    args = parse_arguments()

    benchmark(args)
    plot(args)
