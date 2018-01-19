#include <memory>
#include <string>
#include <utility>
#include "../base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "gtest/gtest.h"
#include "logical_query_plan/join_node.hpp"

#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline_statement.hpp"
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

class SQLPipelineStatementTest : public BaseTest {
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

    _select_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_select_query_a, _select_parse_result.get());

    _multi_statement_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_multi_statement_dependant, _multi_statement_parse_result.get());
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
  const std::string _multi_statement_query = "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  const std::string _multi_statement_dependant = "CREATE VIEW foo AS SELECT * FROM table_a; SELECT * FROM foo;";

  const std::vector<std::string> _join_column_names{"a", "b", "bb"};

  std::shared_ptr<hsql::SQLParserResult> _select_parse_result;
  std::shared_ptr<hsql::SQLParserResult> _multi_statement_parse_result;

  static bool _contains_validate(const std::vector<std::shared_ptr<OperatorTask>>& tasks) {
    for (const auto& task : tasks) {
      if (std::dynamic_pointer_cast<Validate>(task->get_operator())) return true;
    }
    return false;
  }
};

TEST_F(SQLPipelineStatementTest, SimpleCreation) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithoutMVCC) {
  SQLPipelineStatement sql_pipeline{_join_query, false};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipelineStatement sql_pipeline{_select_query_a, context};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreation) {
  SQLPipelineStatement sql_pipeline{_select_parse_result, nullptr, true};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithoutMVCC) {
  SQLPipelineStatement sql_pipeline{_select_parse_result, nullptr, false};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipelineStatement sql_pipeline{_select_parse_result, context, true};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationTooManyStatements) {
  EXPECT_THROW(SQLPipelineStatement(_multi_statement_parse_result, nullptr, false), std::exception);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQL) {
  SQLPipelineStatement sql_pipeline{_select_query_a};
  const auto& parsed_sql = sql_pipeline.get_parsed_sql_statement();

  EXPECT_TRUE(parsed_sql->isValid());

  auto statements = parsed_sql->getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQLMulti) {
  SQLPipelineStatement sql_pipeline{_multi_statement_query};
  EXPECT_THROW(sql_pipeline.get_parsed_sql_statement(), std::exception);
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQP) {
  SQLPipelineStatement sql_pipeline{_join_query};

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPTwice) {
  SQLPipelineStatement sql_pipeline{_join_query};

  sql_pipeline.get_unoptimized_logical_plan();
  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp->subtree_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPNotValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a, false};

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subtree_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQP) {
  SQLPipelineStatement sql_pipeline{_join_query};

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPTwice) {
  SQLPipelineStatement sql_pipeline{_join_query};

  sql_pipeline.get_optimized_logical_plan();
  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp->subtree_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPNotValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a, false};

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subtree_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPDoesNotInfluenceUnoptimizedLQP) {
  SQLPipelineStatement sql_pipeline{_join_query};

  const auto& unoptimized_lqp = sql_pipeline.get_unoptimized_logical_plan();

  // The optimizer works on the original LQP nodes which could be modified during optimization.
  // Copy the structure to check that it is equal after optimizing.
  std::shared_ptr<AbstractLQPNode> unoptimized_copy = unoptimized_lqp->deep_copy();

  // Optimize the LQP node
  sql_pipeline.get_optimized_logical_plan();
  const auto& unoptimized_lqp_new = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_TRUE(subtree_types_are_equal(unoptimized_lqp_new, unoptimized_copy));
}

TEST_F(SQLPipelineStatementTest, GetQueryPlan) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  // We don't have a transaction context yet, as it was not needed
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan->tree_roots();

  // We need the transaction context for the query plan if we use MVCC
  EXPECT_NE(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanTwice) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  sql_pipeline.get_query_plan();
  auto duration = sql_pipeline.compile_time_microseconds();

  const auto& plan = sql_pipeline.get_query_plan();
  auto duration2 = sql_pipeline.compile_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  const auto& roots = plan->tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanJoinWithFilter) {
  SQLPipelineStatement sql_pipeline{_join_query};

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan->tree_roots();

  auto is_join_op = [](const std::shared_ptr<const AbstractOperator>& node) {
    return static_cast<bool>(std::dynamic_pointer_cast<const AbstractJoinOperator>(node));
  };

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
  EXPECT_TRUE(contained_in_query_plan(roots.at(0), is_join_op));
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithMVCC) {
  SQLPipelineStatement sql_pipeline{_select_query_a};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_NE(plan->tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithoutMVCC) {
  SQLPipelineStatement sql_pipeline{_select_query_a, false};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan->tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipelineStatement sql_pipeline{_select_query_a, context};
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan->tree_roots().at(0)->transaction_context().get(), context.get());
}

TEST_F(SQLPipelineStatementTest, GetTasks) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksTwice) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksNotValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a, false};

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_FALSE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetResultTable) {
  SQLPipelineStatement sql_pipeline{_select_query_a};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableTwice) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  sql_pipeline.get_result_table();
  auto duration = sql_pipeline.execution_time_microseconds();

  const auto& table = sql_pipeline.get_result_table();
  auto duration2 = sql_pipeline.execution_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableJoin) {
  SQLPipelineStatement sql_pipeline{_join_query};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableWithScheduler) {
  SQLPipelineStatement sql_pipeline{_join_query};

  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableBadQueryNoMVCC) {
  auto sql = "SELECT a + b FROM table_a";
  SQLPipelineStatement sql_pipeline{sql, false};

  // Make sure this is actually the failed execution and not a logic_error from the transaction management.
  EXPECT_THROW(sql_pipeline.get_result_table(), std::runtime_error);
}

TEST_F(SQLPipelineStatementTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + b FROM table_a";
  SQLPipelineStatement sql_pipeline{sql};

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
  EXPECT_TRUE(sql_pipeline.transaction_context()->aborted());
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 5";
  SQLPipelineStatement sql_pipeline{sql};

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table, otherwise the auto-commit will fail.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoMVCC) {
  SQLPipelineStatement sql_pipeline{_select_query_a, false};

  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);

  // Check that there really is no transaction management
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetTimes) {
  SQLPipelineStatement sql_pipeline{_select_query_a};

  EXPECT_THROW(sql_pipeline.compile_time_microseconds(), std::exception);
  EXPECT_THROW(sql_pipeline.execution_time_microseconds(), std::exception);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(sql_pipeline.compile_time_microseconds().count(), 0);
  EXPECT_GT(sql_pipeline.execution_time_microseconds().count(), 0);
}

TEST_F(SQLPipelineStatementTest, ParseErrorDebugMessage) {
#if !IS_DEBUG
  return;
#endif

  SQLPipelineStatement sql_pipeline{_invalid_sql};
  try {
    sql_pipeline.get_parsed_sql_statement();
    // Fail if the previous command did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the ^ was actually inserted in the error message
    EXPECT_TRUE(error_msg.find('^') != std::string::npos);
  }
}

}  // namespace opossum
