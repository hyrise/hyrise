#include <cxxopts.hpp>

#include "utils/performance_warning.hpp"
#include "scheduler/topology.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/current_scheduler.hpp"
#include "benchmark_runner.hpp"
#include "tpch/tpch_queries.hpp"
#include "types.hpp"

using namespace opossum;

int main(int argc, char* argv[]) {
  // TODO: change this to JSON config
  auto num_iterations = size_t{0};
  auto timeout_duration = size_t{0};
  auto scale_factor = 1.0f;
  auto chunk_size = ChunkOffset(INVALID_CHUNK_OFFSET);
  auto benchmark_mode_str = std::string{"IndividualQueries"};
  auto enable_scheduler = false;
  auto enable_visualization = false;

  cxxopts::Options cli_options_description{"Hyrise Benchmark Runner"};

  // clang-format off
  cli_options_description.add_options()
      ("help", "print this help message")
      ("v,verbose", "Print log messages", cxxopts::value<bool>(verbose_benchmark)->default_value("false"))
      ("r,runs", "Maximum number of runs of a single query", cxxopts::value<size_t>(num_iterations)->default_value("1000")) // NOLINT
      ("c,chunk_size", "ChunkSize, default is 2^32-1", cxxopts::value<ChunkOffset>(chunk_size)->default_value(std::to_string(INVALID_CHUNK_OFFSET))) // NOLINT
      ("t,time", "Maximum seconds within which a new query(set) is initiated", cxxopts::value<size_t>(timeout_duration)->default_value("5")) // NOLINT
      ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>())
      ("m,mode", "IndividualQueries or PermutedQuerySets, default is IndividualQueries", cxxopts::value<std::string>(benchmark_mode_str)->default_value(benchmark_mode_str)) // NOLINT
      ("enable_mvcc", "Enable MVCC", cxxopts::value<bool>()->default_value("false")) // NOLINT
      ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>(enable_scheduler)->default_value("false")) // NOLINT
      ("visualize", "Create a visualization image of one LQP and PQP for each TPCH query", cxxopts::value<bool>(enable_visualization)->default_value("false")) // NOLINT
      ("tables", "Specify tables to load, either a single .csv/.tbl file or a directory with these files", cxxopts::value<std::string>()) // NOLINT
      ("queries", "Specify queries to run, either a single .sql file or a directory with these files", cxxopts::value<std::string>()) // NOLINT
      ("encoding", "Specify Chunk encoding. Options: none, dictionary, runlength, frameofreference (default: dictionary)", cxxopts::value<std::string>()->default_value("dictionary"));  // NOLINT
  // clang-format on

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  // In non-verbose mode, disable performance warnings
  std::optional<PerformanceWarningDisabler> performance_warning_disabler;
  if (!verbose_benchmark) {
    performance_warning_disabler.emplace();
  }

  // Display info about output destination
  std::optional<std::string> output_file_path;
  if (cli_parse_result.count("output") > 0) {
    output_file_path = cli_parse_result["output"].as<std::string>();
    out() << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  } else {
    out() << "- Writing benchmark results to stdout" << std::endl;
  }

  // Display info about MVCC being enabled or not
  out() << "- MVCC is " << (enable_mvcc ? "enabled" : "disabled") << std::endl;

  /**
   * Initialise the Scheduler if the Benchmark was requested to run multithreaded
   */
  if (enable_scheduler) {
    const auto topology = Topology::create_numa_topology();
    out() << "- Running in multi-threaded mode, with the following Topology:" << std::endl;
    topology->print(out());

    const auto scheduler = std::make_shared<NodeQueueScheduler>(topology);
    CurrentScheduler::set(scheduler);
  } else {
    out() << "- Running in single-threaded mode" << std::endl;
  }

  // Build list of query ids to be benchmarked and display it
  std::vector<QueryID> query_ids;
  if (cli_parse_result.count("queries")) {
    const auto cli_query_ids = cli_parse_result["queries"].as<std::vector<QueryID>>();
    for (const auto cli_query_id : cli_query_ids) {
      query_ids.emplace_back(cli_query_id);
    }
  } else {
    std::transform(tpch_queries.begin(), tpch_queries.end(), std::back_inserter(query_ids),
                   [](auto& pair) { return pair.first; });
  }
  out() << "- Benchmarking Queries ";
  for (const auto query_id : query_ids) {
    out() << (query_id) << " ";
  }
  out() << std::endl;

  // Determine benchmark and display it
  auto benchmark_mode = BenchmarkMode::IndividualQueries;  // Just to init it deterministically
  if (benchmark_mode_str == "IndividualQueries") {
    benchmark_mode = BenchmarkMode::IndividualQueries;
  } else if (benchmark_mode_str == "PermutedQuerySets") {
    benchmark_mode = BenchmarkMode::PermutedQuerySets;
  } else {
    std::cerr << "ERROR: Invalid benchmark mode: '" << benchmark_mode_str << "'" << std::endl;
    std::cerr << cli_options_description.help({}) << std::endl;
    return 1;
  }
  out() << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;

  out() << "- Visualization is " << (enable_visualization ? "on" : "off") << std::endl;

  // Run the benchmark
  BenchmarkRunner::create(BenchmarkConfig{}, tables, queries).run();

  return 0;
}



