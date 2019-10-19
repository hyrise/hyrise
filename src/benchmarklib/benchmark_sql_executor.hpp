#pragma once

#include "sql/sql_pipeline.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

class TransactionContext;

// This class provides SQL functionality to BenchmarkItemRunners. See AbstractBenchmarkItemRunner::_on_execute_item().
// If sqlite_wrapper is set (i.e. verification is enabled), all SQL queries executed in the benchmark will also be
// executed using SQLite. Hyrise's and SQLite's results are then compared.
class BenchmarkSQLExecutor {
 public:
  // @param visualize_prefix    Prefix for the filename of the generated query plans (e.g., "TPC-H_6-").
  //                            The suffix will be "LQP/PQP-<statement_idx>.<extension>"
  BenchmarkSQLExecutor(const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                       const std::optional<std::string>& visualize_prefix);

  // This executes the given SQL query, records its metrics and returns a single table (the same as
  // SQLPipeline::get_result_table() would).
  // If visualization and/or verification are enabled, these are transparently done as well.
  std::pair<SQLPipelineStatus, std::shared_ptr<const Table>> execute(
      const std::string& sql, const std::shared_ptr<const Table>& expected_result_table = nullptr);

  // If auto-commit is disabled, explicitly commit / roll back the transaction
  void commit();
  void rollback();

  // Contains one entry per executed SQLPipeline
  std::vector<SQLPipelineMetrics> metrics;

  bool any_verification_failed = false;

  // Can optionally be set by the caller. Otherwise, pipelines are auto-committed
  std::shared_ptr<TransactionContext> transaction_context = nullptr;

 private:
  void _compare_tables(const std::shared_ptr<const Table>& expected_result_table,
                       const std::shared_ptr<const Table>& actual_result_table,
                       const std::optional<const std::string>& description = std::nullopt);
  void _verify_with_sqlite(SQLPipeline& pipeline);
  void _visualize(SQLPipeline& pipeline) const;

  const std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;
  const std::optional<std::string> _visualize_prefix;
};

}  // namespace opossum
