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
  BenchmarkSQLExecutor(bool use_jit, std::shared_ptr<SQLiteWrapper> sqlite_wrapper = nullptr);

  std::shared_ptr<const Table> execute(const std::string& sql);
  std::vector<SQLPipelineMetrics> metrics;
  bool any_verification_failed = false;

 private:
  const bool _use_jit;
  const std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  const std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
