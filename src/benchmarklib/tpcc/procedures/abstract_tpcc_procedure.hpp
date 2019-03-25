#pragma once

#include <random>

#include "concurrency/transaction_context.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "tpcc/tpcc_random_generator.hpp"

namespace opossum {

class AbstractTpccProcedure {
public:
  AbstractTpccProcedure();
  virtual ~AbstractTpccProcedure() = default;

  // TODO Doc
  virtual void execute() = 0;

  virtual char identifier() const = 0;
  virtual std::ostream& print(std::ostream& stream) const = 0;

protected:
  std::shared_ptr<const Table> _execute_sql(std::string sql);

  // TODO add semi-random seed, also to TpccRandomGenerator
  static thread_local std::minstd_rand _random_engine;
  static thread_local TpccRandomGenerator _tpcc_random_generator;
  const std::shared_ptr<TransactionContext> _transaction_context;
};

std::ostream& operator<<(std::ostream& stream, const AbstractTpccProcedure& procedure);

}
