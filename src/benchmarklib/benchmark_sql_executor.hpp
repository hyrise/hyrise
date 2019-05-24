#pragma once

#include "sql/sql_pipeline.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

class TransactionContext;

// This class provides SQL functionality to BenchmarkItemRunners. See AbstractBenchmarkItemRunner::_on_execute_item().
// If sqlite_wrapper is set (i.e. verification is enabled), all SQL queries executed in the benchmark will also be
// executed using SQLite. Hyrise's and SQLite's results are then compared. For now, we expect items to use a
// single transaction, which is why the BenchmarkSQLExecutor executes all queries in the same context.
class BenchmarkSQLExecutor {
 public:
  // @param visualize_prefix    Prefix for the filename of the generated query plans (e.g., "TPC-H_6-").
  //                            The suffix will be "LQP/PQP-<statement_idx>.<extension>"
  BenchmarkSQLExecutor(bool enable_jit, const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                       const std::optional<std::string>& visualize_prefix);

  // This executes the given SQL query, records its metrics and returns a single table (the same as
  // SQLPipeline::get_result_table() would).
  // If visualization and/or verification are enabled, these are transparently done as well.
  std::pair<SQLPipelineStatus, std::shared_ptr<const Table>> execute(const std::string& sql);

  // Contains one entry per executed SQLPipeline
  std::vector<SQLPipelineMetrics> metrics;

  bool any_verification_failed = false;

 private:
  void _verify_with_sqlite(SQLPipeline& pipeline);
  void _visualize(SQLPipeline& pipeline) const;

  const bool _enable_jit;
  const std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;
  const std::optional<std::string> _visualize_prefix;

  const std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
