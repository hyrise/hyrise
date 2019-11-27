#pragma once

#include <random>

#include "benchmark_sql_executor.hpp"
#include "concurrency/transaction_context.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "tpcc/tpcc_random_generator.hpp"

namespace opossum {

class AbstractTPCCProcedure {
 public:
  explicit AbstractTPCCProcedure(BenchmarkSQLExecutor& sql_executor);
  virtual ~AbstractTPCCProcedure() = default;

  AbstractTPCCProcedure(const AbstractTPCCProcedure& other) = default;
  AbstractTPCCProcedure& operator=(const AbstractTPCCProcedure& other);

  // Executes the procedure; returns true if it was successful and false if a transaction conflict occurred
  [[nodiscard]] bool execute();

 protected:
  [[nodiscard]] virtual bool _on_execute() = 0;

  // As random values are generate during creation of the procedure, this is mostly done in a single thread, not in the
  // database worker's. As such, having a fixed seed for all thread-local random engines should not be an issue.
  inline static thread_local std::minstd_rand _random_engine{42};
  inline static thread_local TPCCRandomGenerator _tpcc_random_generator{42};

  BenchmarkSQLExecutor& _sql_executor;
};

}  // namespace opossum
