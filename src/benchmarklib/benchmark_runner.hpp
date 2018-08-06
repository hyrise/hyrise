#pragma once

#include <json.hpp>

#include <chrono>
#include <iostream>
#include <optional>
#include <unordered_map>
#include <vector>

#include "benchmark_utils.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class BenchmarkRunner {
 public:
  BenchmarkRunner(const BenchmarkConfig& config, const NamedQueries& queries, const nlohmann::json& context);

  static BenchmarkRunner create(const BenchmarkConfig& config, const std::string& table_path,
                                const std::string& query_path);

  void run();

  static cxxopts::Options get_basic_cli_options(const std::string& benchmark_name);

  static nlohmann::json create_context(const BenchmarkConfig& config);

 private:
  // Run benchmark in BenchmarkMode::PermutedQuerySets mode
  void _benchmark_permuted_query_sets();

  // Run benchmark in BenchmarkMode::IndividualQueries mode
  void _benchmark_individual_queries();

  void _execute_query(const NamedQuery& named_query);
  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json
  void _create_report(std::ostream& stream) const;

  // Get all the files/tables/queries from a given path
  static std::vector<std::string> _read_table_folder(const std::string& table_path);
  static NamedQueries _read_query_folder(const std::string& query_path);

  static NamedQueries _parse_query_file(const std::string& query_path);

  struct QueryPlans final {
    // std::vector<>s, since queries can contain multiple statements
    std::vector<std::shared_ptr<AbstractLQPNode>> lqps;
    std::vector<std::shared_ptr<SQLQueryPlan>> pqps;
  };

  std::unordered_map<std::string, QueryPlans> _query_plans;

  const BenchmarkConfig _config;

  // NamedQuery = <name, sql>
  const NamedQueries _queries;

  BenchmarkResults _query_results_by_query_name;

  nlohmann::json _context;

  std::optional<PerformanceWarningDisabler> _performance_warning_disabler;

  Duration _total_run_duration{};
};

}  // namespace opossum
