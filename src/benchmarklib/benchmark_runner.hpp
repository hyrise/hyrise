#pragma once

#include <json.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <optional>
#include <unordered_map>
#include <vector>

#include "cxxopts.hpp"

#include "abstract_query_generator.hpp"
#include "abstract_table_generator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "query_benchmark_result.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class SQLPipeline;
class SQLiteWrapper;

class BenchmarkRunner {
 public:
  BenchmarkRunner(const BenchmarkConfig& config, std::unique_ptr<AbstractQueryGenerator> query_generator,
                  std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context);
  ~BenchmarkRunner();

  void run();

  static cxxopts::Options get_basic_cli_options(const std::string& benchmark_name);

  static nlohmann::json create_context(const BenchmarkConfig& config);

 private:
  // Run benchmark in BenchmarkMode::PermutedQuerySet mode
  void _benchmark_permuted_query_set();

  // Run benchmark in BenchmarkMode::IndividualQueries mode
  void _benchmark_individual_queries();

  // Execute warmup run of a query
  void _warmup_query(const QueryID query_id);

  // Calls _schedule_query if the scheduler is active, otherwise calls _execute_query and returns no tasks
  std::vector<std::shared_ptr<AbstractTask>> _schedule_or_execute_query(const QueryID query_id,
                                                                        const std::function<void()>& done_callback);

  // Schedule and return all tasks for named_query
  std::vector<std::shared_ptr<AbstractTask>> _schedule_query(const QueryID query_id,
                                                             const std::function<void()>& done_callback);

  // Execute named_query
  void _execute_query(const QueryID query_id, const std::function<void()>& done_callback);

  // If visualization is enabled, stores an executed plan
  void _store_plan(const QueryID query_id, SQLPipeline& pipeline);

  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json
  void _create_report(std::ostream& stream) const;

  struct QueryPlans final {
    // std::vector<>s, since queries can contain multiple statements
    std::vector<std::shared_ptr<AbstractLQPNode>> lqps;
    std::vector<std::shared_ptr<AbstractOperator>> pqps;
  };

  // If visualization is enabled, this stores the LQP and PQP for each query. Its length is defined by the number of
  // available queries.
  std::vector<QueryPlans> _query_plans;

  const BenchmarkConfig _config;

  std::unique_ptr<AbstractQueryGenerator> _query_generator;
  std::unique_ptr<AbstractTableGenerator> _table_generator;

  // Stores the results of the query executions. Its length is defined by the number of available queries.
  std::vector<QueryBenchmarkResult> _query_results;

  nlohmann::json _context;

  std::optional<PerformanceWarningDisabler> _performance_warning_disabler;

  Duration _total_run_duration{};

  // If the query execution should be validated, this stores a pointer to the used SQLite instance
  std::unique_ptr<SQLiteWrapper> _sqlite_wrapper;
};

}  // namespace opossum
