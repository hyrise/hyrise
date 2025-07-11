#include "abstract_tpcc_procedure.hpp"

#include <random>

#include "benchmark_sql_executor.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "tpcc/tpcc_random_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables):
//   See https://github.com/llvm/llvm-project/issues/47384. Should be fixed with clang-tidy versions >17.
thread_local std::minstd_rand AbstractTPCCProcedure::_random_engine{42};
thread_local TPCCRandomGenerator AbstractTPCCProcedure::_tpcc_random_generator{42};
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

AbstractTPCCProcedure::AbstractTPCCProcedure(BenchmarkSQLExecutor& sql_executor) : _sql_executor(sql_executor) {
  PerformanceWarning(
      "The TPC-C support is in a very early stage. Constraints are not enforced, indexes are often not used, and even "
      "the most obvious optimizations are not done yet.");
}

bool AbstractTPCCProcedure::execute() {
  DebugAssert(!_sql_executor.transaction_context || _sql_executor.transaction_context->is_auto_commit(),
              "The SQLExecutor should not already have a transaction context set");

  // Private to the AbstractTPCCProcedure. The actual procedures should not directly interact with the context.
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  _sql_executor.transaction_context = transaction_context;

  auto success = _on_execute();

  DebugAssert(transaction_context->phase() == TransactionPhase::Committed ||
              transaction_context->phase() == TransactionPhase::RolledBackByUser ||
              transaction_context->phase() == TransactionPhase::RolledBackAfterConflict,
              "Expected TPC-C transaction to either commit or roll back the MVCC transaction");

  return success;
}

// NOLINTNEXTLINE(cert-oop54-cpp): We know that this is not a proper assignment.
AbstractTPCCProcedure& AbstractTPCCProcedure::operator=(const AbstractTPCCProcedure& other) {
  DebugAssert(&_sql_executor == &other._sql_executor,
              "Can only assign AbstractTPCCProcedure if the sql_executors are the same.");
  // Doesn't assign anything as the only member _sql_executor is already the same.
  return *this;
}

}  // namespace hyrise
