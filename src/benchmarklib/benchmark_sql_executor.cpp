#include "benchmark_sql_executor.hpp"

#include "operators/table_scan.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/check_table_equal.hpp"
#include "utils/timer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

namespace hyrise {
BenchmarkSQLExecutor::BenchmarkSQLExecutor(const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper,
                                           const std::optional<std::string>& visualize_prefix)
    : _sqlite_connection(sqlite_wrapper ? std::optional<SQLiteWrapper::Connection>{sqlite_wrapper->new_connection()}
                                        : std::optional<SQLiteWrapper::Connection>{}),
      _visualize_prefix(visualize_prefix) {
  if (_sqlite_connection) {
    _sqlite_connection->raw_execute_query("BEGIN TRANSACTION");
    _sqlite_transaction_open = true;
  }
}

std::pair<SQLPipelineStatus, std::shared_ptr<const Table>> BenchmarkSQLExecutor::execute(
    const std::string& sql, const std::shared_ptr<const Table>& expected_result_table) {
  auto pipeline_builder = SQLPipelineBuilder{sql};
  if (transaction_context) {
    pipeline_builder.with_transaction_context(transaction_context);
  }

  auto pipeline = pipeline_builder.create_pipeline();

  const auto [pipeline_status, result_table] = pipeline.get_result_table();

  // #####################
  // #####################
  // #####################

  auto cardinality_estimator = CardinalityEstimator{};
  cardinality_estimator.guarantee_bottom_up_construction();

  const auto& lqp_plans = pipeline.get_optimized_logical_plans();
  if (lqp_plans.size() != 1) {
    std::cerr << "!!!!!!\n!!!!!!\n!!!!!!\n";
    std::cerr << "!!!!!! Caution: analyzing a pipeline with more than one statement.\n";
    std::cerr << "!!!!!!\n!!!!!!\n!!!!!!\n" << std::endl;
  }

  // Prints the logical query plans. Helps for understanding what's actually happening in the query.
  // We also store visualizations of these plans here: https://hyrise-ci.epic-hpi.de/job/hyrise/job/hyrise/job/master/lastStableBuild/artifact/query_plans/tpch/
  // for (const auto& plan : lqp_plans) {
  //   std::cout << "\n\n\n\n\n###\n###\n###\n" << *plan << "###\n###\n###\n" << std::endl;
  // }

  const auto& pqp_plans = pipeline.get_physical_plans();
  for (const auto& plan_root : pqp_plans) {
    if (plan_root->type() == OperatorType::CreateView || plan_root->type() == OperatorType::DropView) {
      // Skip some case. You might find more cases that need to be discarded.
      std::cout << "Skipping create/drop view operators." << std::endl;
      continue;
    }

    auto op = plan_root->left_input();
    while (op->left_input()) {
      // Print the type of operator when iterating.
      // std::cout << magic_enum::enum_name(plan_root->type()) << std::endl;

      // We only care for filters. Anything else is ignored.
      if (op->type () == OperatorType::TableScan) {
        const auto table_scan_input = op->left_input();
        // We only care about the filters that are directly following a GetTable operations.
        // Again: estimating cardinalities of conjunctive/disjunctive filters is hard and not your task.
        if (table_scan_input->type() == OperatorType::GetTable) {
          // To obtain the sizes, we collect two kinds of information here:
          // (i) for the physical operators, we need to obtain the "performnance data", which stores performance data
          //     for each executed operator. As we throw away the intermediate results of executed queries, you cannot
          //     obtain the size of the intermediate tables after the operator has finished. For that reason: use the
          //     performance data structs.
          // (ii) for the estimated cardinalities during the query optimization, we access the logical query plan (you
          //      can obtain the corresponding logical node for each operator via `->lqp_node`) and feed this logical
          //      node into the cardinality estimator.
          std::cout << "Information for " << *op->lqp_node << std::endl;
          const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op);
          const auto& table_scan_performance_data = table_scan_op->performance_data;
          const auto& get_table_performance_data = table_scan_input->performance_data;
          std::cout << "Input size to table scan: " << get_table_performance_data->output_row_count << std::endl;
          std::cout << "Result size after table scan: " << table_scan_performance_data->output_row_count << std::endl;
          // std::cout << "Actual selectivity of table scan: " << static_cast<float>(get_table_performance_data->output_row_count) / static_cast<float>(table_scan_performance_data->output_row_count) << std::endl;

          std::cout << "Input size to table scan: " << cardinality_estimator.estimate_cardinality(table_scan_input->lqp_node) << std::endl;
          std::cout << "Result size after table scan: " << cardinality_estimator.estimate_cardinality(table_scan_op->lqp_node) << std::endl;

          // std::cout << *(table_scan_op->predicate()) << std::endl;
          // std::cout << magic_enum::enum_name(op->left_input()->type()) << std::endl;
        }
      }
      op = op->left_input();
    }
  }

  // #####################
  // #####################
  // #####################

  if (pipeline_status == SQLPipelineStatus::Failure) {
    return {pipeline_status, nullptr};
  }
  Assert(pipeline_status == SQLPipelineStatus::Success, "Unexpected pipeline status");

  metrics.emplace_back(std::move(pipeline.metrics()));

  if (expected_result_table) {
    _compare_tables(result_table, expected_result_table, "Using dedicated expected result table");
  } else if (_sqlite_connection) {
    _verify_with_sqlite(pipeline);
  }

  if (_visualize_prefix) {
    _visualize(pipeline);
  }

  return {pipeline_status, result_table};
}

BenchmarkSQLExecutor::~BenchmarkSQLExecutor() {
  if (transaction_context) {
    Assert(transaction_context->phase() == TransactionPhase::Committed ||
               transaction_context->phase() == TransactionPhase::RolledBackByUser ||
               transaction_context->phase() == TransactionPhase::RolledBackAfterConflict,
           "Explicitly created transaction context should have been explicitly committed or rolled back");
  }

  // If the benchmark item does not explicitly manage the transaction life time by calling commit or rollback,
  // we automatically commit the sqlite transaction so that the next item can start a new one:
  if (_sqlite_transaction_open) {
    _sqlite_connection->raw_execute_query("COMMIT TRANSACTION");
  }
}

void BenchmarkSQLExecutor::commit() {
  Assert(transaction_context && !transaction_context->is_auto_commit(),
         "Can only explicitly commit transaction if auto-commit is disabled");
  Assert(transaction_context->phase() == TransactionPhase::Active, "Expected transaction to be active");
  transaction_context->commit();
  if (_sqlite_connection) {
    _sqlite_transaction_open = false;
    _sqlite_connection->raw_execute_query("COMMIT TRANSACTION");
  }
}

void BenchmarkSQLExecutor::rollback() {
  Assert(transaction_context && !transaction_context->is_auto_commit(),
         "Can only explicitly roll back transaction if auto-commit is disabled");
  Assert(transaction_context->phase() == TransactionPhase::Active, "Expected transaction to be active");
  transaction_context->rollback(RollbackReason::User);
  if (_sqlite_connection) {
    _sqlite_transaction_open = false;
    _sqlite_connection->raw_execute_query("ROLLBACK TRANSACTION");
  }
}

void BenchmarkSQLExecutor::_verify_with_sqlite(SQLPipeline& pipeline) {
  Assert(pipeline.statement_count() == 1, "Expecting single statement for SQLite verification");

  const auto sqlite_result = _sqlite_connection->execute_query(pipeline.get_sql());
  const auto [pipeline_status, result_table] = pipeline.get_result_table();
  Assert(pipeline_status == SQLPipelineStatus::Success, "Non-successful pipeline should have been caught earlier");

  // Modifications (INSERT, UPDATE, DELETE) do not return a table. We do not know what changed - we do not even know
  // which table has been modified. Extracting that info from the plan and verifying the entire table would take way
  // too long. As such, we rely on any errors here to be found when the potentially corrupted data is SELECTed the
  // next time.
  if (!result_table) {
    return;
  }

  _compare_tables(result_table, sqlite_result, "Using SQLite's result table as expected result table");
}

void BenchmarkSQLExecutor::_compare_tables(const std::shared_ptr<const Table>& actual_result_table,
                                           const std::shared_ptr<const Table>& expected_result_table,
                                           const std::optional<const std::string>& description) {
  Timer timer;

  if (actual_result_table->row_count() > 0) {
    Assert(expected_result_table, "Verifying SQL statement on sqlite failed: No SQLite result");
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

void BenchmarkSQLExecutor::_visualize(SQLPipeline& pipeline) {
  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";

  const auto& lqps = pipeline.get_optimized_logical_plans();
  const auto& pqps = pipeline.get_physical_plans();

  auto prefix = *_visualize_prefix;

  if (_num_visualized_plans == 1) {
    // We have already visualized a prior SQL pipeline in this benchmark item - rename the existing file
    std::filesystem::rename(prefix + "-LQP.svg", prefix + "-0-LQP.svg");
    std::filesystem::rename(prefix + "-PQP.svg", prefix + "-0-PQP.svg");
  }
  if (_num_visualized_plans > 0) {
    prefix += "-" + std::to_string(_num_visualized_plans);
  }

  LQPVisualizer{graphviz_config, {}, {}, {}}.visualize(lqps, prefix + "-LQP.svg");
  PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, prefix + "-PQP.svg");

  ++_num_visualized_plans;
}

}  // namespace hyrise
