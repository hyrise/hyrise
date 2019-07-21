#pragma once

#include <random>

#include "benchmark_sql_executor.hpp"
#include "concurrency/transaction_context.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "tpcc/tpcc_random_generator.hpp"

namespace opossum {

class AbstractTpccProcedure {
 public:
  explicit AbstractTpccProcedure(BenchmarkSQLExecutor& sql_executor);

  AbstractTpccProcedure(const AbstractTpccProcedure& sql_executor);
  virtual ~AbstractTpccProcedure() = default;
  AbstractTpccProcedure& operator=(const AbstractTpccProcedure& sql_executor);

  // Executes the procedure; returns true if it was successful and false if a transaction conflict occurred
  [[nodiscard]] virtual bool execute() = 0;

  // A single character (D/N/O/P/S) used to identify the procedure in result CSVs
  virtual char identifier() const = 0;

 protected:
  static thread_local std::minstd_rand _random_engine;
  static thread_local TpccRandomGenerator _tpcc_random_generator;

  BenchmarkSQLExecutor& _sql_executor;
};

std::ostream& operator<<(std::ostream& stream, const AbstractTpccProcedure& procedure);

}  // namespace opossum
