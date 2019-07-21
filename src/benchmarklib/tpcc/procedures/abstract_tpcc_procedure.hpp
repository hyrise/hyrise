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

  AbstractTPCCProcedure(const AbstractTPCCProcedure& sql_executor);
  virtual ~AbstractTPCCProcedure() = default;
  AbstractTPCCProcedure& operator=(const AbstractTPCCProcedure& sql_executor);

  // Executes the procedure; returns true if it was successful and false if a transaction conflict occurred
  [[nodiscard]] virtual bool execute() = 0;

  // A single character (D/N/O/P/S) used to identify the procedure in result CSVs
  virtual char identifier() const = 0;

 protected:
  static thread_local std::minstd_rand _random_engine;
  static thread_local TPCCRandomGenerator _tpcc_random_generator;

  BenchmarkSQLExecutor& _sql_executor;
};

std::ostream& operator<<(std::ostream& stream, const AbstractTPCCProcedure& procedure);

}  // namespace opossum
