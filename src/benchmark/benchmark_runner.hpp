#pragma once

#include <vector>
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <optional>

#include "storage/encoding_type.hpp"
#include "storage/chunk.hpp"
#include "sql/sql_query_plan.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "benchmark_utils.hpp"

namespace opossum {

class BenchmarkRunner {
 public:
  static BenchmarkRunner create_tpch(BenchmarkConfig config, const std::vector<QueryID>& query_ids = {}, float scale_factor = 1.0f);
  static BenchmarkRunner create(BenchmarkConfig config, const std::string& table_path, const std::string& query_path);

  void run();

 private:
  BenchmarkRunner(BenchmarkConfig config, const NamedQueries& queries);

  // Run benchmark in BenchmarkMode::PermutedQuerySets mode
  void _benchmark_permuted_query_sets();

  // Run benchmark in BenchmarkMode::IndividualQueries mode
  void _benchmark_individual_queries();

  void _execute_query(const NamedQuery& named_query);

  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json
  void _create_report(std::ostream& stream) const;

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
};


}  // namespace opossum


