#include "abstract_tpcc_procedure.hpp"

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

AbstractTPCCProcedure::AbstractTPCCProcedure(BenchmarkSQLExecutor& sql_executor) : _sql_executor(sql_executor) {
  PerformanceWarning(
      "The TPC-C support is in a very early stage. Indexes are not used and even the most obvious optimizations are "
      "not done yet.");
  _sql_executor.transaction_context = TransactionManager::get().new_transaction_context();
}

AbstractTPCCProcedure& AbstractTPCCProcedure::operator=(const AbstractTPCCProcedure& other) {
  DebugAssert(&_sql_executor == &other._sql_executor,
              "Can only assign AbstractTPCCProcedure if the sql_executors are the same");
  // Doesn't assign anything as the only member _sql_executor is already the same.
  return *this;
}

}  // namespace opossum
