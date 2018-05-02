#include <tpch/tpch_db_generator.hpp>
#include <json.hpp>
#include <sql/sql_pipeline_builder.hpp>
#include <random>
#include "benchmark_runner.hpp"
#include "tpch/tpch_queries.hpp"

#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"

namespace opossum {

void BenchmarkRunner::run() {
  // Run the queries in the selected mode
  switch (_config.benchmark_mode) {
    case BenchmarkMode::IndividualQueries:_benchmark_individual_queries();
      break;
    case BenchmarkMode::PermutedQuerySets:_benchmark_permuted_query_sets();
      break;
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
  for (const auto& [name, _] : _queries) {
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
    out() << "- Benchmarking Query " << name << std::endl;

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

  for (const auto& [name, _] : _queries) {
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

  // Generate YY-MM-DD hh:mm::ss
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  nlohmann::json context{
      {"date", timestamp_stream.str()},
//      {"scale_factor", _scale_factor},
      {"chunk_size", _config.chunk_size},
      {"build_type", IS_DEBUG ? "debug" : "release"},
      {"benchmark_mode",
       _config.benchmark_mode == BenchmarkMode::IndividualQueries ? "IndividualQueries" : "PermutedQuerySets"}};

  nlohmann::json report{{"context", context}, {"benchmarks", benchmarks}};

  stream << std::setw(2) << report << std::endl;
}

BenchmarkRunner BenchmarkRunner::create_tpch(BenchmarkConfig config, const std::vector<QueryID>& query_ids, const float scale_factor) {
  NamedQueries queries;
  queries.reserve(query_ids.size());

  for (const auto query_id : query_ids) {
    queries.emplace_back({"TPC-H " + std::to_string(query_id), tpch_queries.at(query_id)});
  }

  out() << "- Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;
  opossum::TpchDbGenerator(scale_factor, config.chunk_size).generate_and_store();
  out() << "- Done." << std::endl;

  return BenchmarkRunner(std::move(config), queries);
}

BenchmarkRunner BenchmarkRunner::create(BenchmarkConfig config, const std::string& table_path, const std::string& query_path) {
  NamedQueries queries;
  return BenchmarkRunner(std::move(config), queries);
}

BenchmarkRunner::BenchmarkRunner(BenchmarkConfig config, const NamedQueries& queries)
    : _config(std::move(config)), _queries(queries) {}

}  // namespace opossum