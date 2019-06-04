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
  AbstractTpccProcedure(BenchmarkSQLExecutor sql_executor);
  virtual ~AbstractTpccProcedure() = default;

  // TODO Doc
  [[nodiscard]] virtual bool execute() = 0;

  virtual char identifier() const = 0;

protected:
  // TODO add semi-random seed, also to TpccRandomGenerator
  static thread_local std::minstd_rand _random_engine;
  static thread_local TpccRandomGenerator _tpcc_random_generator;

  BenchmarkSQLExecutor _sql_executor;
};

std::ostream& operator<<(std::ostream& stream, const AbstractTpccProcedure& procedure);

}
