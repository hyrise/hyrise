#include "benchmark_sql_executor.hpp"

#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/check_table_equal.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

namespace opossum {
BenchmarkSQLExecutor::BenchmarkSQLExecutor(bool enable_jit, const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                                           const std::optional<std::string>& visualize_prefix)
    : _enable_jit(enable_jit),
      _sqlite_wrapper(sqlite_wrapper),
      _visualize_prefix(visualize_prefix),
      _transaction_context(TransactionManager::get().new_transaction_context()) {}

std::shared_ptr<const Table> BenchmarkSQLExecutor::execute(const std::string& sql) {
  auto builder = SQLPipelineBuilder{sql};
  if (_visualize_prefix) builder.dont_cleanup_temporaries();
  builder.with_transaction_context(_transaction_context);
  if (_enable_jit) builder.with_lqp_translator(std::make_shared<JitAwareLQPTranslator>());
  auto pipeline = builder.create_pipeline();

  const auto& result_table = pipeline.get_result_table();
  metrics.emplace_back(std::move(pipeline.metrics()));

  if (_sqlite_wrapper) {
    _verify_with_sqlite(pipeline);
  }

  if (_visualize_prefix) {
    _visualize(pipeline);
  }

  return result_table;
}

void BenchmarkSQLExecutor::_verify_with_sqlite(SQLPipeline& pipeline) {
  const auto sqlite_result = _sqlite_wrapper->execute_query(pipeline.get_sql());
  const auto& result_table = pipeline.get_result_table();

  if (result_table->row_count() > 0) {
    if (sqlite_result->row_count() == 0) {
      any_verification_failed = true;
      std::cout << "- Verification failed: Hyrise returned a result, but SQLite did not" << std::endl;
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

void BenchmarkSQLExecutor::_visualize(SQLPipeline& pipeline) const {
  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";

  const auto& lqps = pipeline.get_optimized_logical_plans();
  const auto& pqps = pipeline.get_physical_plans();

  for (auto lqp_idx = size_t{0}; lqp_idx < lqps.size(); ++lqp_idx) {
    const auto file_prefix = *_visualize_prefix + "-LQP-" + std::to_string(lqp_idx);
    LQPVisualizer{graphviz_config, {}, {}, {}}.visualize({lqps[lqp_idx]}, file_prefix + ".svg");
  }

  for (auto pqp_idx = size_t{0}; pqp_idx < pqps.size(); ++pqp_idx) {
    const auto file_prefix = *_visualize_prefix + "-PQP-" + std::to_string(pqp_idx);
    PQPVisualizer{graphviz_config, {}, {}, {}}.visualize({pqps[pqp_idx]}, file_prefix + ".svg");
  }
}

}  // namespace opossum
