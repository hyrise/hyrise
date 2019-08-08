#include "benchmark_sql_executor.hpp"

#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/check_table_equal.hpp"
#include "utils/timer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

namespace opossum {
BenchmarkSQLExecutor::BenchmarkSQLExecutor(bool enable_jit, const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                                           const std::optional<std::string>& visualize_prefix)
    : _enable_jit(enable_jit), _sqlite_wrapper(sqlite_wrapper), _visualize_prefix(visualize_prefix) {}

std::pair<SQLPipelineStatus, std::shared_ptr<const Table>> BenchmarkSQLExecutor::execute(
    const std::string& sql, const std::shared_ptr<const Table>& expected_result_table) {
  auto pipeline_builder = SQLPipelineBuilder{sql};
  if (_visualize_prefix) pipeline_builder.dont_cleanup_temporaries();
  if (transaction_context) pipeline_builder.with_transaction_context(transaction_context);
  if (_enable_jit) pipeline_builder.with_lqp_translator(std::make_shared<JitAwareLQPTranslator>());

  auto pipeline = pipeline_builder.create_pipeline();

  const auto [pipeline_status, result_table] = pipeline.get_result_table();

  if (pipeline_status == SQLPipelineStatus::RolledBack) {
    return {pipeline_status, nullptr};
  }
  DebugAssert(pipeline_status == SQLPipelineStatus::Success, "Unexpected pipeline status");

  metrics.emplace_back(std::move(pipeline.metrics()));

  if (expected_result_table) {
    _compare_tables(expected_result_table, result_table, "Using dedicated expected result table");
  } else if (_sqlite_wrapper) {
    _verify_with_sqlite(pipeline);
  }

  if (_visualize_prefix) {
    _visualize(pipeline);
  }

  return {pipeline_status, result_table};
}

void BenchmarkSQLExecutor::commit() {
  DebugAssert(transaction_context, "Can only explicitly commit transaction if auto-commit is disabled");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active, "Expected transaction to be active");
  transaction_context->commit();
}

void BenchmarkSQLExecutor::rollback() {
  DebugAssert(transaction_context, "Can only explicitly roll back transaction if auto-commit is disabled");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active, "Expected transaction to be active");
  transaction_context->rollback();
}

void BenchmarkSQLExecutor::_verify_with_sqlite(SQLPipeline& pipeline) {
  Assert(pipeline.statement_count() == 1, "Expecting single statement for SQLite verification");

  const auto sqlite_result = _sqlite_wrapper->execute_query(pipeline.get_sql());
  const auto [pipeline_status, result_table] = pipeline.get_result_table();
  DebugAssert(pipeline_status == SQLPipelineStatus::Success, "Non-successful pipeline should have been caught earlier");
  _compare_tables(sqlite_result, result_table, "Using SQLite's result table as expected result table");
}

void BenchmarkSQLExecutor::_compare_tables(const std::shared_ptr<const Table>& expected_result_table,
                                           const std::shared_ptr<const Table>& actual_result_table,
                                           const std::optional<const std::string>& description) {
  Timer timer;

  if (actual_result_table->row_count() > 0) {
    if (expected_result_table->row_count() == 0) {
      any_verification_failed = true;
      if (description) {
        std::cout << "- " + *description << "\n";
      }
      std::cout << "- Verification failed: Hyrise's actual result is not empty, but the expected result is ("
                << timer.lap_formatted() << ")"
                << "\n";
    } else if (const auto table_difference_message = check_table_equal(
                   actual_result_table, expected_result_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                   FloatComparisonMode::RelativeDifference, IgnoreNullable::Yes)) {
      any_verification_failed = true;
      if (description) {
        std::cout << *description << "\n";
      }
      std::cout << "- Verification failed (" << timer.lap_formatted() << ")"
                << "\n"
                << *table_difference_message << "\n";
    }
  } else {
    if (expected_result_table && expected_result_table->row_count() > 0) {
      any_verification_failed = true;
      if (description) {
        std::cout << *description << "\n";
      }
      std::cout << "- Verification failed: Expected result table is not empty, but Hyrise's actual result is ("
                << timer.lap_formatted() << ")"
                << "\n";
    }
  }
}

void BenchmarkSQLExecutor::_visualize(SQLPipeline& pipeline) const {
  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";

  const auto& lqps = pipeline.get_optimized_logical_plans();
  const auto& pqps = pipeline.get_physical_plans();

  LQPVisualizer{graphviz_config, {}, {}, {}}.visualize(lqps, *_visualize_prefix + "-LQP.svg");
  PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, *_visualize_prefix + "-PQP.svg");
}

}  // namespace opossum
