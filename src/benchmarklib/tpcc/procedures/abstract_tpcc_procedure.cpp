#include "abstract_tpcc_procedure.hpp"

#include "hyrise.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

AbstractTPCCProcedure::AbstractTPCCProcedure(BenchmarkSQLExecutor& sql_executor) : _sql_executor(sql_executor) {
  PerformanceWarning(
      "The TPC-C support is in a very early stage. Indexes are not used and even the most obvious optimizations are "
      "not done yet.");
}

bool AbstractTPCCProcedure::execute() {
  DebugAssert(!_sql_executor.transaction_context, "The SQLExecutor should not already have a transaction context set");

  // Private to the AbstractTPCCProcedure. The actual procedures should not directly interact with the context.
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
  _sql_executor.transaction_context = transaction_context;

  auto success = _on_execute();

  DebugAssert(transaction_context->phase() == TransactionPhase::Committed ||
                  transaction_context->phase() == TransactionPhase::RolledBack,
              "Expected TPC-C transaction to either commit or roll back the MVCC transaction");

  return success;
}

// NOLINTNEXTLINE - we know that this is not a proper assignment
AbstractTPCCProcedure& AbstractTPCCProcedure::operator=(const AbstractTPCCProcedure& other) {
  DebugAssert(&_sql_executor == &other._sql_executor,
              "Can only assign AbstractTPCCProcedure if the sql_executors are the same");
  // Doesn't assign anything as the only member _sql_executor is already the same.
  return *this;
}

}  // namespace opossum
