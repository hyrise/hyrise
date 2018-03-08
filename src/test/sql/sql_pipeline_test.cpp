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
#include "sql/sql_pipeline.hpp"
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

    _table_a_multi = load_table("src/test/tables/int_float.tbl", 2);
    _table_a_multi->append({11, 11.11});
    StorageManager::get().add_table("table_a_multi", _table_a_multi);

    _table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _table_b);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Float);
    column_definitions.emplace_back("bb", DataType::Float);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);
    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_a_multi;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _fail_query = "SELECT * FROM table_does_not_exist";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
  const std::string _multi_statement_query = "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  const std::string _multi_statement_dependent = "CREATE VIEW foo AS SELECT * FROM table_a; SELECT * FROM foo;";
  // VIEW --> VIE
  const std::string _multi_statement_invalid = "CREATE VIE foo AS SELECT * FROM table_a; SELECT * FROM foo;";
};

TEST_F(SQLPipelineTest, SimpleCreation) {
  SQLPipeline sql_pipeline{_select_query_a};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCC) {
  SQLPipeline sql_pipeline{_select_query_a, UseMvcc::No};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipeline sql_pipeline{_select_query_a, context};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationMulti) {
  SQLPipeline sql_pipeline{_multi_statement_query};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCCMulti) {
  SQLPipeline sql_pipeline{_multi_statement_query, UseMvcc::No};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContextMulti) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipeline sql_pipeline{_multi_statement_query, context};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationInvalid) {
  EXPECT_THROW(SQLPipeline sql_pipeline{_multi_statement_invalid}, std::exception);
}

TEST_F(SQLPipelineTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options
  const auto optimizer = Optimizer::create_default_optimizer();
  auto prepared_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // No transaction context
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, optimizer, UseMvcc::Yes));
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, prepared_cache, UseMvcc::No));
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, optimizer, prepared_cache, UseMvcc::Yes));

  // With transaction context
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, optimizer, transaction_context));
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, prepared_cache, transaction_context));
  EXPECT_NO_THROW(SQLPipeline(_select_query_a, optimizer, prepared_cache, transaction_context));
}

TEST_F(SQLPipelineTest, GetParsedSQLStatements) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 1u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};
  EXPECT_NO_THROW(sql_pipeline.get_parsed_sql_statements());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 2u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
  EXPECT_TRUE(parsed_sql_statements.back()->isValid());
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPs) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPsMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};

  try {
    sql_pipeline.get_unoptimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot translate") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetOptimizedLQP) {
  SQLPipeline sql_pipeline{_select_query_a};

  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPsMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};

  try {
    sql_pipeline.get_optimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot translate") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetQueryPlans) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& plans = sql_pipeline.get_query_plans();

  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& plans = sql_pipeline.get_query_plans();

  EXPECT_EQ(plans.size(), 2u);
}

TEST_F(SQLPipelineTest, GetQueryPlanTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_query_plans();
  auto duration = sql_pipeline.compile_time_microseconds();

  const auto& plans = sql_pipeline.get_query_plans();
  auto duration2 = sql_pipeline.compile_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};
  try {
    sql_pipeline.get_query_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot compile") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetTasks) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
}

TEST_F(SQLPipelineTest, GetTasksTwice) {
  SQLPipeline sql_pipeline{_select_query_a};

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};

  try {
    sql_pipeline.get_tasks();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot generate tasks") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetResultTable) {
  SQLPipeline sql_pipeline{_select_query_a};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableMultiple) {
  SQLPipeline sql_pipeline{_multi_statement_query};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a_multi)
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

TEST_F(SQLPipelineTest, GetResultTableExecutionRequired) {
  SQLPipeline sql_pipeline{_multi_statement_dependent};
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableWithScheduler) {
  SQLPipeline sql_pipeline{_join_query};

  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + b FROM table_a";
  SQLPipeline sql_pipeline{sql};

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
}

TEST_F(SQLPipelineTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 150";
  SQLPipeline sql_pipeline{sql};

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineTest, GetTimes) {
  SQLPipeline sql_pipeline{_select_query_a};

  EXPECT_THROW(sql_pipeline.compile_time_microseconds(), std::exception);
  EXPECT_THROW(sql_pipeline.execution_time_microseconds(), std::exception);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(sql_pipeline.compile_time_microseconds().count(), 0);
  EXPECT_GT(sql_pipeline.execution_time_microseconds().count(), 0);
}

TEST_F(SQLPipelineTest, GetFailedPipelineUnoptimizedLQPs) {
  SQLPipeline sql_pipeline{_fail_query};

  try {
    sql_pipeline.get_unoptimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception&) {
    EXPECT_NE(sql_pipeline.failed_pipeline_statement(), nullptr);
  }
}

TEST_F(SQLPipelineTest, GetFailedPipelineOptimizedLQPs) {
  SQLPipeline sql_pipeline{_fail_query};

  try {
    sql_pipeline.get_optimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception&) {
    EXPECT_NE(sql_pipeline.failed_pipeline_statement(), nullptr);
  }
}

TEST_F(SQLPipelineTest, GetFailedPipelineGueryPlans) {
  SQLPipeline sql_pipeline{_fail_query};

  try {
    sql_pipeline.get_query_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception&) {
    EXPECT_NE(sql_pipeline.failed_pipeline_statement(), nullptr);
  }
}

TEST_F(SQLPipelineTest, GetFailedPipelineResultTable) {
  SQLPipeline sql_pipeline{_fail_query};

  try {
    sql_pipeline.get_result_table();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception&) {
    EXPECT_NE(sql_pipeline.failed_pipeline_statement(), nullptr);
  }
}

TEST_F(SQLPipelineTest, RequiresExecutionVariations) {
  EXPECT_FALSE(SQLPipeline{_select_query_a}.requires_execution());
  EXPECT_FALSE(SQLPipeline{_join_query}.requires_execution());
  EXPECT_FALSE(SQLPipeline{_multi_statement_query}.requires_execution());
  EXPECT_TRUE(SQLPipeline{_multi_statement_dependent}.requires_execution());

  const std::string create_view_single = "CREATE VIEW blub AS SELECT * FROM foo;";
  EXPECT_FALSE(SQLPipeline{create_view_single}.requires_execution());

  const std::string create_view_multi_reverse = "SELECT * FROM blub; " + create_view_single;
  EXPECT_TRUE(SQLPipeline{create_view_multi_reverse}.requires_execution());

  const std::string create_view_multi_middle = create_view_multi_reverse + " SELECT * FROM foo;";
  EXPECT_TRUE(SQLPipeline{create_view_multi_reverse}.requires_execution());

  const std::string create_table_single = "CREATE TABLE foo2 (c int);";
  EXPECT_FALSE(SQLPipeline{create_table_single}.requires_execution());

  const std::string create_table_multi = create_table_single + "SELECT * FROM foo2;";
  EXPECT_TRUE(SQLPipeline{create_table_multi}.requires_execution());

  const std::string drop_table_single = "DROP TABLE foo;";
  EXPECT_FALSE(SQLPipeline{drop_table_single}.requires_execution());

  const std::string drop_table_multi = "SELECT * FROM foo; " + drop_table_single;
  EXPECT_TRUE(SQLPipeline{drop_table_multi}.requires_execution());

  const std::string multi_no_exec =
      "SELECT * FROM foo; INSERT INTO foo VALUES (2); SELECT * FROM blub; DELETE FROM foo WHERE a = 2;";
  EXPECT_FALSE(SQLPipeline{multi_no_exec}.requires_execution());
}

TEST_F(SQLPipelineTest, CorrectStatementStringSplitting) {
  // Tests that the string passed into the pipeline is correctly split into the statement substrings
  SQLPipeline select_pipeline{_select_query_a};
  const auto& select_strings = select_pipeline.get_sql_strings();
  EXPECT_EQ(select_strings.size(), 1u);
  EXPECT_EQ(select_strings.at(0), _select_query_a);

  SQLPipeline dependent_pipeline{_multi_statement_query};
  const auto& dependent_strings = dependent_pipeline.get_sql_strings();
  EXPECT_EQ(dependent_strings.size(), 2u);
  // "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  EXPECT_EQ(dependent_strings.at(0), "INSERT INTO table_a VALUES (11, 11.11);");
  EXPECT_EQ(dependent_strings.at(1), "SELECT * FROM table_a");  // leading whitespace should be removed

  // Add newlines, tabd and weird spacing
  auto spacing_sql = "\n\t\n SELECT\na, b, c,d,e FROM\t(SELECT * FROM foo);    \t  ";
  SQLPipeline spacing_pipeline{spacing_sql};
  const auto& spacing_strings = spacing_pipeline.get_sql_strings();
  EXPECT_EQ(spacing_strings.size(), 1u);
  EXPECT_EQ(spacing_strings.at(0),
            "SELECT\na, b, c,d,e FROM\t(SELECT * FROM foo);");  // internal formatting is not done

  auto multi_line_sql = R"(
  SELECT *
  FROM foo, bar
  WHERE foo.x = 17
    AND bar.y = 25
  ORDER BY foo.x ASC
  )";
  SQLPipeline multi_line_pipeline{multi_line_sql};
  const auto& multi_line_strings = multi_line_pipeline.get_sql_strings();
  EXPECT_EQ(multi_line_strings.size(), 1u);
  EXPECT_EQ(multi_line_strings.at(0),
            "SELECT *\n  FROM foo, bar\n  WHERE foo.x = 17\n    AND bar.y = 25\n  ORDER BY foo.x ASC");
}

TEST_F(SQLPipelineTest, CacheQueryPlanTwice) {
  SQLPipeline sql_pipeline1{_select_query_a};
  sql_pipeline1.get_result_table();

  // INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a
  SQLPipeline sql_pipeline2{_multi_statement_query};
  sql_pipeline2.get_result_table();

  // The second part of _multi_statement_query is _select_query_a, which is already cached
  const auto& cache = SQLQueryCache<SQLQueryPlan>::get();
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_TRUE(cache.has(_select_query_a));
  EXPECT_TRUE(cache.has("INSERT INTO table_a VALUES (11, 11.11);"));

  SQLPipeline sql_pipeline3{_select_query_a};
  sql_pipeline3.get_result_table();

  // Make sure the cache hasn't changed
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_TRUE(cache.has(_select_query_a));
  EXPECT_TRUE(cache.has("INSERT INTO table_a VALUES (11, 11.11);"));
}

}  // namespace opossum
