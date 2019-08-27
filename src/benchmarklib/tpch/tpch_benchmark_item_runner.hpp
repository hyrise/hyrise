#pragma once

#include <atomic>

#include "abstract_benchmark_item_runner.hpp"

namespace opossum {

// We want to provide both "classical" TPC-H queries (i.e., regular SQL queries), and prepared statements. To do so,
// we use tpch_queries.cpp as a basis and either build PREPARE and EXECUTE statements or replace the question marks
// with their random values before returning the SQL query.
class TPCHBenchmarkItemRunner : public AbstractBenchmarkItemRunner {
 public:
  // Constructor for a TPCHBenchmarkItemRunner containing all TPC-H queries
  TPCHBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, bool use_prepared_statements,
                          float scale_factor);

  // Constructor for a TPCHBenchmarkItemRunner containing a subset of TPC-H queries
  TPCHBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, bool use_prepared_statements,
                          float scale_factor, const std::vector<BenchmarkItemID>& items);

  void on_tables_loaded() override;

  std::string item_name(const BenchmarkItemID item_id) const override;
  const std::vector<BenchmarkItemID>& items() const override;

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) override;

  // Runs the PREPARE queries if _use_prepared_statements is set, otherwise does nothing
  void _prepare_queries() const;

  // Returns an SQL query with random parameters for a given (zero-indexed) benchmark item (i.e., 0 -> TPC-H 1)
  std::string _build_query(const BenchmarkItemID item_id);

  // Same as build_query, but uses the same parameters every time. Good for tests.
  std::string _build_deterministic_query(const BenchmarkItemID item_id);

  // Runs either an EXECUTE query or fills the "?" placeholders with values and returns the resulting SQL string,
  // depending on _use_prepared_statements
  std::string _substitute_placeholders(const BenchmarkItemID item_id, const std::vector<std::string>& parameter_values);

  // Should we use prepared statements or generate "regular" SQL queries?
  const bool _use_prepared_statements;

  const float _scale_factor;

  // Used for naming the views generated in query 15
  std::atomic<size_t> _q15_view_id = 0;

  // We want deterministic seeds, but since the engine is thread-local, we need to make sure that each thread has its
  // own seed.
  std::atomic<unsigned int> _random_seed{0};

  std::vector<BenchmarkItemID> _items;

  friend class TPCHTest;
};

}  // namespace opossum
