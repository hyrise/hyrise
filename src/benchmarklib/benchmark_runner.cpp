#include <random>

#if __has_include(<fs>)
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
#include <scheduler/current_scheduler.hpp>
namespace fs = std::experimental::filesystem;
#endif

#include <json.hpp>
#include <storage/chunk_encoder.hpp>

#include "benchmark_runner.hpp"
#include "constant_mappings.hpp"
#include "import_export/csv_parser.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BenchmarkRunner::BenchmarkRunner(BenchmarkConfig config, const NamedQueries& queries, const nlohmann::json& context)
    : _config(std::move(config)), _queries(queries), _context(context) {}

void BenchmarkRunner::run() {
  _config.out << "\n- Starting Benchmark.." << std::endl;

  // Run the queries in the selected mode
  switch (_config.benchmark_mode) {
    case BenchmarkMode::IndividualQueries: {
      _benchmark_individual_queries();
      break;
    }
    case BenchmarkMode::PermutedQuerySets: {
      _benchmark_permuted_query_sets();
      break;
    }
  }

  // Create report
  if (_config.output_file_path) {
    std::ofstream output_file(*_config.output_file_path);
    _create_report(output_file);
  } else {
    _create_report(std::cout);
  }

  // Visualize query plans
  if (_config.enable_visualization) {
    for (const auto& name_and_plans : _query_plans) {
      const auto& name = name_and_plans.first;
      const auto& lqps = name_and_plans.second.lqps;
      const auto& pqps = name_and_plans.second.pqps;

      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";

      for (auto lqp_idx = size_t{0}; lqp_idx < lqps.size(); ++lqp_idx) {
        const auto file_prefix = name + "-LQP-" + std::to_string(lqp_idx);
        LQPVisualizer{graphviz_config, {}, {}, {}}.visualize({lqps[lqp_idx]}, file_prefix + ".dot",
                                                             file_prefix + ".svg");
      }
      for (auto pqp_idx = size_t{0}; pqp_idx < pqps.size(); ++pqp_idx) {
        const auto file_prefix = name + "-PQP-" + std::to_string(pqp_idx);
        SQLQueryPlanVisualizer{graphviz_config, {}, {}, {}}.visualize(*pqps[pqp_idx], file_prefix + ".dot",
                                                                      file_prefix + ".svg");
      }
    }
  }
}

void BenchmarkRunner::_benchmark_permuted_query_sets() {
  // Init results
  for (const auto& named_query : _queries) {
    const auto& name = named_query.first;
    _query_results_by_query_name.emplace(name, QueryBenchmarkResult{});
  }

  auto mutable_named_queries = _queries;

  // For shuffling the query order
  std::random_device random_device;
  std::mt19937 random_generator(random_device());

  BenchmarkState state{_config.max_num_query_runs, _config.max_duration};
  while (state.keep_running()) {
    std::shuffle(mutable_named_queries.begin(), mutable_named_queries.end(), random_generator);

    for (const auto& named_query : mutable_named_queries) {
      const auto query_benchmark_begin = std::chrono::steady_clock::now();

      // Execute the query, we don't care about the results
      _execute_query(named_query);

      const auto query_benchmark_end = std::chrono::steady_clock::now();

      auto& query_benchmark_result = _query_results_by_query_name.at(named_query.first);
      query_benchmark_result.duration += query_benchmark_end - query_benchmark_begin;
      query_benchmark_result.num_iterations++;
    }
  }
}

void BenchmarkRunner::_benchmark_individual_queries() {
  for (const auto& named_query : _queries) {
    const auto& name = named_query.first;
    _config.out << "- Benchmarking Query " << name << std::endl;

    BenchmarkState state{_config.max_num_query_runs, _config.max_duration};
    while (state.keep_running()) {
      _execute_query(named_query);
    }

    QueryBenchmarkResult result;
    result.num_iterations = state.num_iterations;
    result.duration = std::chrono::high_resolution_clock::now() - state.begin;

    _query_results_by_query_name.emplace(name, result);
  }
}
void BenchmarkRunner::_execute_query(const NamedQuery& named_query) {
  const auto& name = named_query.first;
  const auto& sql = named_query.second;

  auto pipeline = SQLPipelineBuilder{sql}.with_mvcc(_config.use_mvcc).create_pipeline();
  // Execute the query, we don't care about the results
  pipeline.get_result_table();

  // If necessary, keep plans for visualization
  if (_config.enable_visualization) {
    const auto query_plans_iter = _query_plans.find(name);
    if (query_plans_iter == _query_plans.end()) {
      Assert(pipeline.get_query_plans().size() == 1, "Expected exactly one SQLQueryPlan");
      QueryPlans plans{pipeline.get_optimized_logical_plans(), pipeline.get_query_plans()};
      _query_plans.emplace(name, plans);
    }
  }
}

void BenchmarkRunner::_create_report(std::ostream& stream) const {
  nlohmann::json benchmarks;

  for (const auto& named_query : _queries) {
    const auto& name = named_query.first;
    const auto& query_result = _query_results_by_query_name.at(name);

    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(query_result.duration).count();
    const auto duration_seconds = static_cast<float>(duration_ns) / 1'000'000'000;
    const auto items_per_second = static_cast<float>(query_result.num_iterations) / duration_seconds;
    const auto time_per_query = duration_ns / query_result.num_iterations;

    nlohmann::json benchmark{
        {"name", name},
        {"iterations", query_result.num_iterations},
        {"real_time", time_per_query},
        {"cpu_time", time_per_query},
        {"items_per_second", items_per_second},
        {"time_unit", "ns"},
    };

    benchmarks.push_back(benchmark);
  }

  nlohmann::json report{{"context", _context}, {"benchmarks", benchmarks}};

  stream << std::setw(2) << report << std::endl;
}

BenchmarkRunner BenchmarkRunner::create_tpch(BenchmarkConfig config, const std::vector<QueryID>& query_ids,
                                             const float scale_factor) {
  NamedQueries queries;
  queries.reserve(query_ids.size());

  for (const auto query_id : query_ids) {
    queries.emplace_back("TPC-H " + std::to_string(query_id), tpch_queries.at(query_id));
  }

  config.out << "- Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;

  ColumnEncodingSpec encoding_spec{config.encoding_type};
  const auto tables = opossum::TpchDbGenerator(scale_factor, config.chunk_size).generate();

  for (auto& table : tables) {
    if (config.encoding_type != EncodingType::Unencoded) {
      ChunkEncoder::encode_all_chunks(table.second, encoding_spec);
    }

    StorageManager::get().add_table(tpch_table_names.at(table.first), table.second);
  }
  config.out << "- Done." << std::endl;

  auto context = _create_context(config);
  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);

  return BenchmarkRunner(std::move(config), queries, context);
}

BenchmarkRunner BenchmarkRunner::create(BenchmarkConfig config, const std::string& table_path,
                                        const std::string& query_path) {
  const auto tables = _parse_table_path(table_path);
  if (tables.empty()) {
    throw std::runtime_error("No tables found in '" + table_path + "'");
  }

  CsvMeta csv_meta{};
  csv_meta.chunk_size = config.chunk_size;

  ColumnEncodingSpec encoding_spec{config.encoding_type};

  for (const auto& table_str : tables) {
    const auto table_name = fs::path{table_str}.stem().string();

    std::shared_ptr<Table> table;
    if (boost::algorithm::ends_with(table_str, ".tbl")) {
      table = load_table(table_str, config.chunk_size);
    } else {
      table = CsvParser{}.parse(table_str, csv_meta);
    }

    if (config.encoding_type != EncodingType::Unencoded) {
      ChunkEncoder::encode_all_chunks(table, encoding_spec);
    }

    StorageManager::get().add_table(table_name, table);
    config.out << "- Adding table '" << table_name << "'" << std::endl;
  }

  const auto queries = _parse_query_path(query_path);
  return BenchmarkRunner(config, queries, _create_context(config));
}

std::vector<std::string> BenchmarkRunner::_parse_table_path(const std::string& table_path) {
  const auto is_table_file = [](const std::string& filename) {
    return (boost::algorithm::ends_with(filename, ".csv") || boost::algorithm::ends_with(filename, ".tbl"));
  };

  fs::path path{table_path};
  if (!fs::exists(path)) {
    throw std::runtime_error("No such file or directory '" + table_path + "'");
  }

  std::vector<std::string> tables;

  // If only one file was specified, add it and return
  if (fs::is_regular_file(path)) {
    if (is_table_file(table_path)) {
      tables.push_back(table_path);
      return tables;
    } else {
      throw std::runtime_error("Specified file '" + table_path + "' is not a .csv or .tbl file");
    }
  }

  // Recursively walk through the specified directory and add all files on the way
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    const auto filename = entry.path().string();
    if (fs::is_regular_file(entry) && is_table_file(filename)) {
      tables.push_back(filename);
    }
  }

  return tables;
}

NamedQueries BenchmarkRunner::_parse_query_path(const std::string& query_path) {
  const auto is_sql_file = [](const std::string& filename) { return boost::algorithm::ends_with(filename, ".sql"); };

  fs::path path{query_path};
  if (!fs::exists(path)) {
    throw std::runtime_error("No such file or directory '" + query_path + "'");
  }

  if (fs::is_regular_file(path)) {
    if (is_sql_file(query_path)) {
      return _parse_query_file(query_path);
    } else {
      throw std::runtime_error("Specified file '" + query_path + "' is not an .sql file");
    }
  }

  // Recursively walk through the specified directory and add all files on the way
  NamedQueries queries;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    const auto filename = entry.path().string();
    if (fs::is_regular_file(entry) && is_sql_file(filename)) {
      const auto file_queries = _parse_query_file(filename);
      queries.insert(queries.end(), file_queries.begin(), file_queries.end());
    }
  }

  return queries;
}

NamedQueries BenchmarkRunner::_parse_query_file(const std::string& query_path) {
  auto query_id = 0u;

  std::ifstream file(query_path);
  const auto filename = fs::path{query_path}.stem().string();

  NamedQueries queries;
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }

    const auto query_name = filename + '.' + std::to_string(query_id);
    queries.emplace_back(query_name, std::move(query));
    query_id++;
  }

  return queries;
}

cxxopts::Options BenchmarkRunner::get_default_cli_options(const std::string& benchmark_name) {
  cxxopts::Options cli_options{benchmark_name};

  // clang-format off
  cli_options.add_options()
    ("help", "print this help message")
    ("v,verbose", "Print log messages", cxxopts::value<bool>()->default_value("false"))
    ("r,runs", "Maximum number of runs of a single query", cxxopts::value<size_t>()->default_value("1000")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 2^32-1", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::MAX_SIZE))) // NOLINT
    ("t,time", "Maximum seconds within which a new query(set) is initiated", cxxopts::value<size_t>()->default_value("5")) // NOLINT
    ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>())
    ("m,mode", "IndividualQueries or PermutedQuerySets, default is IndividualQueries", cxxopts::value<std::string>()->default_value("IndividualQueries")) // NOLINT
    ("e,encoding", "Specify Chunk encoding. Options: none, dictionary, runlength, frameofreference (default: dictionary)", cxxopts::value<std::string>()->default_value("dictionary"))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("mvcc", "Enable MVCC", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("visualize", "Create a visualization image of one LQP and PQP for each query", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  return cli_options;
}

BenchmarkConfig BenchmarkRunner::parse_default_cli_options(const cxxopts::ParseResult& parse_result,
                                                           const cxxopts::Options& cli_options) {
  // Should the benchmark be run in verbose mode
  const auto verbose = parse_result["verbose"].as<bool>();
  auto& out = get_out_stream(verbose);

  // In non-verbose mode, disable performance warnings
  std::optional<PerformanceWarningDisabler> performance_warning_disabler;
  if (!verbose) {
    performance_warning_disabler.emplace();
  }

  // Display info about output destination
  std::optional<std::string> output_file_path;
  if (parse_result.count("output") > 0) {
    output_file_path = parse_result["output"].as<std::string>();
    out << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  } else {
    out << "- Writing benchmark results to stdout" << std::endl;
  }

  // Display info about MVCC being enabled or not
  const auto enable_mvcc = parse_result["mvcc"].as<bool>();
  const auto use_mvcc = enable_mvcc ? UseMvcc::Yes : UseMvcc::No;
  out << "- MVCC is " << (enable_mvcc ? "enabled" : "disabled") << std::endl;

  // Initialise the scheduler if the benchmark was requested to run multi-threaded
  const auto enable_scheduler = parse_result["scheduler"].as<bool>();
  if (enable_scheduler) {
    const auto topology = Topology::create_numa_topology();
    out << "- Running in multi-threaded mode, with the following Topology:" << std::endl;
    topology->print(out);

    const auto scheduler = std::make_shared<NodeQueueScheduler>(topology);
    CurrentScheduler::set(scheduler);
  } else {
    out << "- Running in single-threaded mode" << std::endl;
  }

  // Determine benchmark and display it
  const auto benchmark_mode_str = parse_result["mode"].as<std::string>();
  auto benchmark_mode = BenchmarkMode::IndividualQueries;  // Just to init it deterministically
  if (benchmark_mode_str == "IndividualQueries") {
    benchmark_mode = BenchmarkMode::IndividualQueries;
  } else if (benchmark_mode_str == "PermutedQuerySets") {
    benchmark_mode = BenchmarkMode::PermutedQuerySets;
  } else {
    std::cerr << cli_options.help({}) << std::endl;
    throw std::runtime_error("Invalid benchmark mode: '" + benchmark_mode_str + "'");
  }
  out << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;

  const auto enable_visualization = parse_result["visualize"].as<bool>();
  out << "- Visualization is " << (enable_visualization ? "on" : "off") << std::endl;

  // Get the specified encoding type
  const auto encoding_type_str = parse_result["encoding"].as<std::string>();
  auto encoding_type = EncodingType::Dictionary;  // Just to init it deterministically
  if (encoding_type_str == "dictionary") {
    encoding_type = EncodingType::Dictionary;
  } else if (encoding_type_str == "runlength") {
    encoding_type = EncodingType::RunLength;
  } else if (encoding_type_str == "frameofreference") {
    encoding_type = EncodingType::FrameOfReference;
  } else if (encoding_type_str == "none") {
    encoding_type = EncodingType::Unencoded;
  } else {
    std::cerr << cli_options.help({}) << std::endl;
    throw std::runtime_error("Invalid encoding type: '" + encoding_type_str + "'");
  }

  out << "- Encoding is '" << encoding_type_str << "'" << std::endl;

  // Get all other variables
  const auto chunk_size = parse_result["chunk_size"].as<ChunkOffset>();
  out << "- Chunk size is " << chunk_size << std::endl;

  const auto max_runs = parse_result["runs"].as<size_t>();
  out << "- Max runs per query is " << max_runs << std::endl;

  const auto max_duration = parse_result["time"].as<size_t>();
  out << "- Max duration per query is " << max_duration << " seconds" << std::endl;
  const Duration timeout_duration = std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{max_duration});

  return BenchmarkConfig{
      benchmark_mode, verbose,          chunk_size,       encoding_type,        max_runs, timeout_duration,
      use_mvcc,       output_file_path, enable_scheduler, enable_visualization, out};
}
nlohmann::json BenchmarkRunner::_create_context(const BenchmarkConfig& config) {
  // Generate YY-MM-DD hh:mm::ss
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  const auto encoding_string = encoding_type_to_string.at(config.encoding_type);

  return nlohmann::json{
      {"date", timestamp_stream.str()},
      {"chunk_size", config.chunk_size},
      {"build_type", IS_DEBUG ? "debug" : "release"},
      {"encoding", encoding_string},
      {"benchmark_mode",
       config.benchmark_mode == BenchmarkMode::IndividualQueries ? "IndividualQueries" : "PermutedQuerySets"}};
}

}  // namespace opossum
