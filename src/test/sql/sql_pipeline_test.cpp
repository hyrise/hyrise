#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLPipelineTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    _table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _table_b);

    _join_result = std::make_shared<Table>();
    _join_result->add_column("a", DataType::Int);
    _join_result->add_column("b", DataType::Float);
    _join_result->add_column("bb", DataType::Float);
    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});
  }

  bool _contains_validate(const std::shared_ptr<AbstractLQPNode>& node) {
    if (node == nullptr) return false;
    if (node->type() == LQPNodeType::Validate) return true;
    return _contains_validate(node->left_child()) || _contains_validate(node->right_child());
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
};

TEST_F(SQLPipelineTest, SimpleCreation) {
  SQLPipeline sql_pipeline{_select_query_a};

  EXPECT_NE(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.sql_string(), _select_query_a);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCC) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipeline sql_pipeline{_select_query_a, context};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.sql_string(), _select_query_a);
}

TEST_F(SQLPipelineTest, GetParsedSQL) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& parsed_sql = sql_pipeline.get_parsed_sql();

  EXPECT_TRUE(parsed_sql.isValid());

  auto statements = parsed_sql.getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineTest, GetParsedSQLTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_parsed_sql();
  auto duration = sql_pipeline.parse_time_seconds();

  const auto& parsed_sql = sql_pipeline.get_parsed_sql();
  auto duration2 = sql_pipeline.parse_time_seconds();

  // Make sure this was not un twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TRUE(parsed_sql.isValid());

  auto statements = parsed_sql.getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineTest, GetParsedSQLInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_parsed_sql(), std::exception);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQP) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPWithJoinFilter) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
  EXPECT_EQ(lqp_roots.at(0)->left_child()->type(), LQPNodeType::Predicate);
  EXPECT_EQ(lqp_roots.at(0)->left_child()->left_child()->type(), LQPNodeType::Predicate);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_unoptimized_logical_plans(), std::exception);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPValidated) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_TRUE(_contains_validate(lqp_roots.at(0)));
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_FALSE(_contains_validate(lqp_roots.at(0)));
}

TEST_F(SQLPipelineTest, GetOptimizedLQP) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_optimized_logical_plans();
  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1ul);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPWithJoinFilter) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->type(), LQPNodeType::Projection);
  EXPECT_EQ(lqp_roots.at(0)->left_child()->type(), LQPNodeType::Predicate);
  EXPECT_EQ(lqp_roots.at(0)->left_child()->left_child()->type(), LQPNodeType::Join);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_optimized_logical_plans(), std::exception);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPValidated) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_TRUE(_contains_validate(lqp_roots.at(0)));
}

TEST_F(SQLPipelineTest, GetOptimizedLQPNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_FALSE(_contains_validate(lqp_roots.at(0)));
}

TEST_F(SQLPipelineTest, GetQueryPlan) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan.tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_EQ(roots.at(0)->name(), "Projection");
}

TEST_F(SQLPipelineTest, GetQueryPlanTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_query_plan();
  auto duration = sql_pipeline.compile_time_seconds();

  const auto& plan = sql_pipeline.get_query_plan();
  auto duration2 = sql_pipeline.compile_time_seconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  const auto& roots = plan.tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_EQ(roots.at(0)->name(), "Projection");
}

TEST_F(SQLPipelineTest, GetQueryPlanJoinWithFilter) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan.tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_EQ(roots.at(0)->name(), "Projection");
  EXPECT_EQ(roots.at(0)->input_left()->name(), "TableScan");
  EXPECT_TRUE(roots.at(0)->input_left()->input_left()->name().find("Join") != std::string::npos);
}

TEST_F(SQLPipelineTest, GetQueryPlanInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_query_plan(), std::exception);
}

TEST_F(SQLPipelineTest, GetQueryPlanWithMVCC) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_NE(plan.tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineTest, GetQueryPlanWithoutMVCC) {
  SQLPipeline sql_pipeline{_select_query_a, false};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan.tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineTest, GetQueryPlanWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipeline sql_pipeline{_select_query_a, context};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan.tree_roots().at(0)->transaction_context().get(), context.get());
}

TEST_F(SQLPipelineTest, GetTasks) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_EQ(tasks.at(0)->get_operator()->name(), "GetTable");
  EXPECT_EQ(tasks.at(1)->get_operator()->name(), "Validate");
  EXPECT_EQ(tasks.at(2)->get_operator()->name(), "Projection");
}

TEST_F(SQLPipelineTest, GetTasksTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_EQ(tasks.at(0)->get_operator()->name(), "GetTable");
  EXPECT_EQ(tasks.at(1)->get_operator()->name(), "Validate");
  EXPECT_EQ(tasks.at(2)->get_operator()->name(), "Projection");
}

TEST_F(SQLPipelineTest, GetTasksNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_EQ(tasks.at(0)->get_operator()->name(), "GetTable");
  EXPECT_EQ(tasks.at(1)->get_operator()->name(), "Projection");
}

TEST_F(SQLPipelineTest, GetResultTable) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_result_table();
  auto duration = sql_pipeline.execution_time_seconds();

  const auto& table = sql_pipeline.get_result_table();
  auto duration2 = sql_pipeline.execution_time_seconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableJoin) {
  SQLPipeline sql_pipeline{_join_query};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineTest, GetResultTableWithScheduler) {
  SQLPipeline sql_pipeline{_join_query};

  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineTest, GetResultTableBadQueryNoMVCC) {
  auto sql = "SELECT a + b FROM table_a";
  SQLPipeline sql_pipeline{sql, false};

  // Make sure this is actually the failed execution and not a logic_error from the transaction management.
  EXPECT_THROW(sql_pipeline.get_result_table(), std::runtime_error);
}

TEST_F(SQLPipelineTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + b FROM table_a";
  SQLPipeline sql_pipeline{sql};

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
  EXPECT_TRUE(sql_pipeline.transaction_context()->aborted());
}

TEST_F(SQLPipelineTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 5";
  SQLPipeline sql_pipeline{sql};

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table, otherwise the auto-commit will fail.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineTest, GetResultTableNoMVCC) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);

  // Check that there really is no transaction management
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineTest, GetTimes) {
  SQLPipeline sql_pipeline{_select_query_a};

  EXPECT_THROW(sql_pipeline.parse_time_seconds(), std::exception);
  EXPECT_THROW(sql_pipeline.compile_time_seconds(), std::exception);
  EXPECT_THROW(sql_pipeline.execution_time_seconds(), std::exception);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(sql_pipeline.parse_time_seconds().count(), 0.0f);
  EXPECT_GT(sql_pipeline.compile_time_seconds().count(), 0.0f);
  EXPECT_GT(sql_pipeline.execution_time_seconds().count(), 0.0f);
}

}  // namespace opossum
