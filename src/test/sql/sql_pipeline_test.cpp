#include <memory>
#include <string>
#include <utility>
#include "../base_test.hpp"

#include "SQLParser.h"
#include "gtest/gtest.h"
#include "logical_query_plan/join_node.hpp"

#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace {
// This function is a slightly hacky way to check whether an LQP was optimized. This relies on JoinDetectionRule and
// could break if something is changed within the optimizer.
// It assumes that for the query: SELECT * from a, b WHERE a.a = b.a will be translated to a Cross Join with a filter
// predicate and then optimized to a Join.
std::function<bool(const std::shared_ptr<opossum::AbstractLQPNode>&)> contains_cross =
    [](const std::shared_ptr<opossum::AbstractLQPNode>& node) {
      if (node->type() != opossum::LQPNodeType::Join) return false;
      if (auto join_node = std::dynamic_pointer_cast<opossum::JoinNode>(node)) {
        return join_node->join_mode() == opossum::JoinMode::Cross;
      }
      return false;
    };
}  // namespace

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

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";

  const std::vector<std::string> _join_column_names{"a", "b", "bb"};

  static bool _contains_validate(const std::vector<std::shared_ptr<OperatorTask>>& tasks) {
    for (const auto& task : tasks) {
      if (std::dynamic_pointer_cast<Validate>(task->get_operator())) return true;
    }
    return false;
  }
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
  auto duration = sql_pipeline.parse_time_microseconds();

  const auto& parsed_sql = sql_pipeline.get_parsed_sql();
  auto duration2 = sql_pipeline.parse_time_microseconds();

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
  SQLPipeline sql_pipeline{_join_query};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp_roots.at(0), contains_cross));
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_join_query};

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp_roots.at(0), contains_cross));
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_unoptimized_logical_plans(), std::exception);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPValidated) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_TRUE(lqp_roots.at(0)->subtree_is_validated());
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_FALSE(lqp_roots.at(0)->subtree_is_validated());
}

TEST_F(SQLPipelineTest, GetOptimizedLQP) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp_roots.at(0), contains_cross));
}

TEST_F(SQLPipelineTest, GetOptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_join_query};

  sql_pipeline.get_optimized_logical_plans();
  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_EQ(lqp_roots.at(0)->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp_roots.at(0), contains_cross));
}

TEST_F(SQLPipelineTest, GetOptimizedLQPInvalid) {
  SQLPipeline sql_pipeline{_invalid_sql};
  EXPECT_THROW(sql_pipeline.get_optimized_logical_plans(), std::exception);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPValidated) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_TRUE(lqp_roots.at(0)->subtree_is_validated());
}

TEST_F(SQLPipelineTest, GetOptimizedLQPNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& lqp_roots = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqp_roots.size(), 1u);
  EXPECT_FALSE(lqp_roots.at(0)->subtree_is_validated());
}

TEST_F(SQLPipelineTest, GetOptimizedLQPDoesNotInfluenceUnoptimizedLQP) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& unoptimized_lqp_roots = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(unoptimized_lqp_roots.size(), 1u);

  // The optimizer works on the original LQP nodes which could be modified during optimization.
  // Copy the structure to check that it is equal after optimizing.
  std::shared_ptr<AbstractLQPNode> unoptimized_copy = unoptimized_lqp_roots.at(0)->deep_copy();

  // Optimize the LQP node
  sql_pipeline.get_optimized_logical_plans();
  const auto& unoptimized_lqp_roots_new = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(unoptimized_lqp_roots_new.size(), 1u);
  EXPECT_TRUE(subtree_types_are_equal(unoptimized_lqp_roots_new.at(0), unoptimized_copy));
}

TEST_F(SQLPipelineTest, GetQueryPlan) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan.tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineTest, GetQueryPlanTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_query_plan();
  auto duration = sql_pipeline.compile_time_microseconds();

  const auto& plan = sql_pipeline.get_query_plan();
  auto duration2 = sql_pipeline.compile_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  const auto& roots = plan.tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineTest, GetQueryPlanJoinWithFilter) {
  SQLPipeline sql_pipeline{_join_query};

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan.tree_roots();

  auto is_join_op = [](const std::shared_ptr<const AbstractOperator>& node) {
    return static_cast<bool>(std::dynamic_pointer_cast<const AbstractJoinOperator>(node));
  };

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
  EXPECT_TRUE(contained_in_query_plan(roots.at(0), is_join_op));
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

  const auto& tasks_sets = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks_sets.size(), 1u);
  EXPECT_EQ(tasks_sets.at(0).size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks_sets.at(0)));
}

TEST_F(SQLPipelineTest, GetTasksTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_tasks();
  const auto& tasks_sets = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks_sets.size(), 1u);
  EXPECT_EQ(tasks_sets.at(0).size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks_sets.at(0)));
}

TEST_F(SQLPipelineTest, GetTasksNotValidated) {
  SQLPipeline sql_pipeline{_select_query_a, false};

  const auto& task_sets = sql_pipeline.get_tasks();

  EXPECT_EQ(task_sets.size(), 1u);
  EXPECT_EQ(task_sets.at(0).size(), 2u);
  EXPECT_FALSE(_contains_validate(task_sets.at(0)));
}

TEST_F(SQLPipelineTest, GetResultTable) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_result_table();
  auto duration = sql_pipeline.execution_time_microseconds();

  const auto& table = sql_pipeline.get_result_table();
  auto duration2 = sql_pipeline.execution_time_microseconds();

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

  EXPECT_THROW(sql_pipeline.parse_time_microseconds(), std::exception);
  EXPECT_THROW(sql_pipeline.compile_time_microseconds(), std::exception);
  EXPECT_THROW(sql_pipeline.execution_time_microseconds(), std::exception);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(sql_pipeline.parse_time_microseconds().count(), 0);
  EXPECT_GT(sql_pipeline.compile_time_microseconds().count(), 0);
  EXPECT_GT(sql_pipeline.execution_time_microseconds().count(), 0);
}

}  // namespace opossum
