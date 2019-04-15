#include "benchmark_sql_executor.hpp"

#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/check_table_equal.hpp"

namespace opossum {
BenchmarkSQLExecutor::BenchmarkSQLExecutor(bool use_jit, std::shared_ptr<SQLiteWrapper> sqlite_wrapper)
    : _use_jit(use_jit), _sqlite_wrapper(sqlite_wrapper), _transaction_context(TransactionManager::get().new_transaction_context()) {}

std::shared_ptr<const Table> BenchmarkSQLExecutor::execute(const std::string& sql) {
  auto builder = SQLPipelineBuilder{sql};
  builder.with_transaction_context(_transaction_context);
  if(_use_jit) builder.with_lqp_translator(std::make_shared<JitAwareLQPTranslator>());
  auto pipeline = builder.create_pipeline();

  auto result_table = pipeline.get_result_table();
  Print::print(result_table);
  metrics.emplace_back(std::move(pipeline.metrics()));

  if (_sqlite_wrapper) {
    // TODO Make this AcId compliant
    const auto sqlite_result = _sqlite_wrapper->execute_query(sql);

    if (result_table->row_count() > 0) {
      if (sqlite_result->row_count() == 0) {
        any_verification_failed = true;
        std::cout << "- Verification failed: Hyrise returned a result, but SQLite didn't" << std::endl;
      } else if (!check_table_equal(result_table, sqlite_result, OrderSensitivity::No, TypeCmpMode::Lenient,
                                    FloatComparisonMode::RelativeDifference)) {
        any_verification_failed = true;
        std::cout << "- Verification failed: Tables are not equal" << std::endl;
      }
    } else {
      if (sqlite_result && sqlite_result->row_count() > 0) {
        any_verification_failed = true;
        std::cout << "- Verification failed: SQLite returned a result, but Hyrise did not" << std::endl;
      }
    }
  }

  return result_table;
}

}  // namespace opossum