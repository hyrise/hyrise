#pragma once

#include "sql/sql_pipeline.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

class TransactionContext;

// This class provides SQL functionality to BenchmarkItemRunners. See AbstractBenchmarkItemRunner:_execute_item.
class BenchmarkSQLExecutor {
 public:
  // If sqlite_wrapper is set, all SQL queries executed in the benchmark will also be executed using SQLite and then
  // validated.
  BenchmarkSQLExecutor(bool enable_jit, const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                       const std::optional<std::string>& visualize_prefix);

  std::shared_ptr<const Table> execute(const std::string& sql);
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
