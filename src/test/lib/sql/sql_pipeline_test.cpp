#include <memory>
#include <string>
#include <utility>

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "base_test.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"

namespace hyrise {

class SQLPipelineTest : public BaseTest {
 protected:
  static void SetUpTestCase() {  // called ONCE before the tests
    _table_a_multi = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    _table_a_multi->append({11, 11.11f});
    _table_b = load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2});

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, false);
    column_definitions.emplace_back("bb", DataType::Float, false);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);
    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});
  }

  void SetUp() override {
    // We reload table_a every time since it is modified during the test case.
    _table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    Hyrise::get().storage_manager.add_table("table_a_multi", _table_a_multi);
    Hyrise::get().storage_manager.add_table("table_b", _table_b);

    _pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  }

  // Tables modified during test case
  std::shared_ptr<Table> _table_a;

  // Tables not modified during test case
  inline static std::shared_ptr<Table> _table_a_multi;
  inline static std::shared_ptr<Table> _table_b;
  inline static std::shared_ptr<Table> _join_result;

  std::shared_ptr<SQLPhysicalPlanCache> _pqp_cache;

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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContext) {
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationMulti) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCCMulti) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.disable_mvcc().create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContextMulti) {
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.with_transaction_context(context).create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationInvalid) {
  EXPECT_THROW(auto sql_pipeline = SQLPipelineBuilder{_multi_statement_invalid}.create_pipeline(), std::exception);
}

TEST_F(SQLPipelineTest, ParseErrorDebugMessage) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  try {
    auto sql_pipeline = SQLPipelineBuilder{_invalid_sql}.create_pipeline();
    // Fail if the previous command did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the ^ was actually inserted in the error message
    EXPECT_TRUE(error_msg.find('^') != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options
  const auto optimizer = Optimizer::create_default_optimizer();
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // No transaction context
  EXPECT_NO_THROW(
      SQLPipelineBuilder(_select_query_a).with_optimizer(optimizer).with_mvcc(UseMvcc::Yes).create_pipeline());
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a).with_mvcc(UseMvcc::No).create_pipeline());
  EXPECT_NO_THROW(
      SQLPipelineBuilder(_select_query_a).with_optimizer(optimizer).with_mvcc(UseMvcc::Yes).create_pipeline());

  // With transaction context
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_transaction_context(transaction_context)
                      .with_optimizer(optimizer)
                      .with_mvcc(UseMvcc::Yes)
                      .create_pipeline());
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_transaction_context(transaction_context)
                      .with_optimizer(optimizer)
                      .with_mvcc(UseMvcc::Yes)
                      .create_pipeline());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatements) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 1u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline.get_parsed_sql_statements());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 2u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
  EXPECT_TRUE(parsed_sql_statements.back()->isValid());
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPs) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& plans = sql_pipeline.get_physical_plans();

  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& plans = sql_pipeline.get_physical_plans();

  EXPECT_EQ(plans.size(), 2u);
}

TEST_F(SQLPipelineTest, GetQueryPlanTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();

  sql_pipeline.get_physical_plans();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration = metrics.statement_metrics[0]->lqp_translation_duration;

  const auto& plans = sql_pipeline.get_physical_plans();
  auto duration2 = metrics.statement_metrics[0]->lqp_translation_duration;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  try {
    sql_pipeline.get_physical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot compile") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetTasks) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
}

TEST_F(SQLPipelineTest, GetTasksTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineTest, GetResultTablesMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();

  const auto& [pipeline_status, tables] = sql_pipeline.get_result_tables();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  EXPECT_EQ(tables[0], nullptr);

  EXPECT_TABLE_EQ_UNORDERED(tables[1], _table_a_multi);
}

TEST_F(SQLPipelineTest, GetResultTableMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a_multi);
}

TEST_F(SQLPipelineTest, GetResultTableTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();

  sql_pipeline.get_result_table();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration = metrics.statement_metrics[0]->plan_execution_duration;

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration2 = metrics.statement_metrics[0]->plan_execution_duration;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineTest, GetResultTableExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineTest, GetResultTableWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();

  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + not_a_column FROM table_a";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
}

TEST_F(SQLPipelineTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 150";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table.
  const auto& [pipeline_status2, table2] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status2, SQLPipelineStatus::Success);
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineTest, UpdateWithTransactionFailure) {
  // Mark a row as modified by a different transaction
  auto first_chunk_mvcc_data = _table_a->get_chunk(ChunkID{0})->mvcc_data();

  first_chunk_mvcc_data->set_tid(ChunkOffset{1}, TransactionID{17});

  const auto sql =
      "UPDATE table_a SET a = 1 WHERE a = 12345; UPDATE table_a SET a = 1 WHERE a = 123; "
      "UPDATE table_a SET a = 1 WHERE a = 1234";
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{sql}.with_transaction_context(transaction_context).create_pipeline();

  const auto [pipeline_status, tables] = sql_pipeline.get_result_tables();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Failure);
  EXPECT_EQ(tables.size(), 0);
  EXPECT_EQ(sql_pipeline.failed_pipeline_statement()->get_sql_string(), "UPDATE table_a SET a = 1 WHERE a = 123;");
  EXPECT_TRUE(transaction_context->aborted());

  // No row should have been touched
  EXPECT_EQ(first_chunk_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{0});
  EXPECT_EQ(first_chunk_mvcc_data->get_end_cid(ChunkOffset{0}), MAX_COMMIT_ID);

  EXPECT_EQ(first_chunk_mvcc_data->get_tid(ChunkOffset{1}), TransactionID{17});
  EXPECT_EQ(first_chunk_mvcc_data->get_end_cid(ChunkOffset{1}), MAX_COMMIT_ID);

  auto second_chunk_mvcc_data = _table_a->get_chunk(ChunkID{1})->mvcc_data();

  EXPECT_EQ(second_chunk_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{0});
  EXPECT_EQ(second_chunk_mvcc_data->get_end_cid(ChunkOffset{0}), MAX_COMMIT_ID);
}

TEST_F(SQLPipelineTest, UpdateWithTransactionFailureAutoCommit) {
  // Similar to UpdateWithTransactionFailure, but without explicit transaction context

  // Mark a row as modified by a different transaction
  auto first_chunk_mvcc_data = _table_a->get_chunk(ChunkID{0})->mvcc_data();

  first_chunk_mvcc_data->set_tid(ChunkOffset{1}, TransactionID{17});

  const auto sql =
      "UPDATE table_a SET a = 1 WHERE a = 12345; UPDATE table_a SET a = 1 WHERE a = 123; "
      "UPDATE table_a SET a = 1 WHERE a = 1234";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  const auto& [pipeline_status, tables] = sql_pipeline.get_result_tables();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Failure);
  EXPECT_EQ(tables.size(), 1);
  EXPECT_EQ(sql_pipeline.failed_pipeline_statement()->get_sql_string(), "UPDATE table_a SET a = 1 WHERE a = 123;");

  // This time, the first row should have been updated before the second statement failed
  EXPECT_EQ(first_chunk_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{1});
  EXPECT_EQ(first_chunk_mvcc_data->get_end_cid(ChunkOffset{0}), CommitID{2});  // initial commit ID + 1

  // This row was being modified by a different transaction, so it should not have been touched
  EXPECT_EQ(first_chunk_mvcc_data->get_tid(ChunkOffset{1}), TransactionID{17});
  EXPECT_EQ(first_chunk_mvcc_data->get_end_cid(ChunkOffset{1}), MAX_COMMIT_ID);

  // We had to abort before we got to the third statement
  auto second_chunk_mvcc_data = _table_a->get_chunk(ChunkID{1})->mvcc_data();

  EXPECT_EQ(second_chunk_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{0});
  EXPECT_EQ(second_chunk_mvcc_data->get_end_cid(ChunkOffset{0}), MAX_COMMIT_ID);
}

TEST_F(SQLPipelineTest, GetTimes) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  const auto& statement_metrics = metrics.statement_metrics[0];

  const auto zero_duration = std::chrono::nanoseconds::zero();

  EXPECT_EQ(statement_metrics->sql_translation_duration, zero_duration);
  EXPECT_EQ(statement_metrics->optimization_duration, zero_duration);
  EXPECT_EQ(statement_metrics->lqp_translation_duration, zero_duration);
  EXPECT_EQ(statement_metrics->plan_execution_duration, zero_duration);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(metrics.parse_time_nanos, zero_duration);
  EXPECT_GT(statement_metrics->sql_translation_duration, zero_duration);
  EXPECT_GT(statement_metrics->optimization_duration, zero_duration);
  EXPECT_GT(statement_metrics->lqp_translation_duration, zero_duration);
  EXPECT_GT(statement_metrics->plan_execution_duration, zero_duration);
}

TEST_F(SQLPipelineTest, RequiresExecutionVariations) {
  EXPECT_FALSE(SQLPipelineBuilder{_select_query_a}.create_pipeline().requires_execution());
  EXPECT_FALSE(SQLPipelineBuilder{_join_query}.create_pipeline().requires_execution());
  EXPECT_FALSE(SQLPipelineBuilder{_multi_statement_query}.create_pipeline().requires_execution());
  EXPECT_TRUE(SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline().requires_execution());

  const std::string create_view_single = "CREATE VIEW blub AS SELECT * FROM foo;";
  EXPECT_FALSE(SQLPipelineBuilder{create_view_single}.create_pipeline().requires_execution());

  const std::string create_view_multi_reverse = "SELECT * FROM blub; " + create_view_single;
  EXPECT_TRUE(SQLPipelineBuilder{create_view_multi_reverse}.create_pipeline().requires_execution());

  const std::string create_view_multi_middle = create_view_multi_reverse + " SELECT * FROM foo;";
  EXPECT_TRUE(SQLPipelineBuilder{create_view_multi_reverse}.create_pipeline().requires_execution());

  const std::string create_table_single = "CREATE TABLE foo2 (c int);";
  EXPECT_FALSE(SQLPipelineBuilder{create_table_single}.create_pipeline().requires_execution());

  const std::string create_table_multi = create_table_single + "SELECT * FROM foo2;";
  EXPECT_TRUE(SQLPipelineBuilder{create_table_multi}.create_pipeline().requires_execution());

  const std::string drop_table_single = "DROP TABLE foo;";
  EXPECT_FALSE(SQLPipelineBuilder{drop_table_single}.create_pipeline().requires_execution());

  const std::string drop_table_multi = "SELECT * FROM foo; " + drop_table_single;
  EXPECT_TRUE(SQLPipelineBuilder{drop_table_multi}.create_pipeline().requires_execution());

  const std::string multi_no_exec =
      "SELECT * FROM foo; INSERT INTO foo VALUES (2); SELECT * FROM blub; DELETE FROM foo WHERE a = 2;";
  EXPECT_FALSE(SQLPipelineBuilder{multi_no_exec}.create_pipeline().requires_execution());
}

TEST_F(SQLPipelineTest, CorrectStatementStringSplitting) {
  // Tests that the string passed into the pipeline is correctly split into the statement substrings
  auto select_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& select_strings = select_pipeline.get_sql_per_statement();
  EXPECT_EQ(select_strings.size(), 1u);
  EXPECT_EQ(select_strings.at(0), _select_query_a);

  auto dependent_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& dependent_strings = dependent_pipeline.get_sql_per_statement();
  EXPECT_EQ(dependent_strings.size(), 2u);
  // "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  EXPECT_EQ(dependent_strings.at(0), "INSERT INTO table_a VALUES (11, 11.11);");
  EXPECT_EQ(dependent_strings.at(1), "SELECT * FROM table_a");  // leading whitespace should be removed

  // Add newlines, tabd and weird spacing
  auto spacing_sql = "\n\t\n SELECT\na, b, c,d,e FROM\t(SELECT * FROM foo);    \t  ";
  auto spacing_pipeline = SQLPipelineBuilder{spacing_sql}.create_pipeline();
  const auto& spacing_strings = spacing_pipeline.get_sql_per_statement();
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
  auto multi_line_pipeline = SQLPipelineBuilder{multi_line_sql}.create_pipeline();
  const auto& multi_line_strings = multi_line_pipeline.get_sql_per_statement();
  EXPECT_EQ(multi_line_strings.size(), 1u);
  EXPECT_EQ(multi_line_strings.at(0),
            "SELECT *\n  FROM foo, bar\n  WHERE foo.x = 17\n    AND bar.y = 25\n  ORDER BY foo.x ASC");
}

TEST_F(SQLPipelineTest, CacheQueryPlanTwice) {
  auto sql_pipeline1 = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  sql_pipeline1.get_result_table();

  // INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a
  auto sql_pipeline2 = SQLPipelineBuilder{_multi_statement_query}.with_pqp_cache(_pqp_cache).create_pipeline();
  sql_pipeline2.get_result_table();

  // The second part of _multi_statement_query is _select_query_a, which is already cached
  EXPECT_EQ(_pqp_cache->size(), 2u);
  EXPECT_TRUE(_pqp_cache->has(_select_query_a));
  EXPECT_TRUE(_pqp_cache->has("INSERT INTO table_a VALUES (11, 11.11);"));

  auto sql_pipeline3 = SQLPipelineBuilder{_select_query_a}.with_pqp_cache(_pqp_cache).create_pipeline();
  sql_pipeline3.get_result_table();

  // Make sure the cache hasn't changed
  EXPECT_EQ(_pqp_cache->size(), 2u);
  EXPECT_TRUE(_pqp_cache->has(_select_query_a));
  EXPECT_TRUE(_pqp_cache->has("INSERT INTO table_a VALUES (11, 11.11);"));
}

TEST_F(SQLPipelineTest, DefaultPlanCaches) {
  const auto default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  const auto local_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  const auto default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  const auto local_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  // No caches
  const auto sql_pipeline_0 = SQLPipelineBuilder{"SELECT * FROM table_a"}.create_pipeline();
  EXPECT_FALSE(sql_pipeline_0.pqp_cache);
  EXPECT_FALSE(sql_pipeline_0.lqp_cache);

  // Default caches
  Hyrise::get().default_pqp_cache = default_pqp_cache;
  Hyrise::get().default_lqp_cache = default_lqp_cache;
  const auto sql_pipeline_1 = SQLPipelineBuilder{"SELECT * FROM table_a"}.create_pipeline();
  EXPECT_EQ(sql_pipeline_1.pqp_cache, default_pqp_cache);
  EXPECT_EQ(sql_pipeline_1.lqp_cache, default_lqp_cache);

  // Local caches
  const auto sql_pipeline_2 = SQLPipelineBuilder{"SELECT * FROM table_a"}
                                  .with_pqp_cache(local_pqp_cache)
                                  .with_lqp_cache(local_lqp_cache)
                                  .create_pipeline();
  EXPECT_EQ(sql_pipeline_2.pqp_cache, local_pqp_cache);
  EXPECT_EQ(sql_pipeline_2.lqp_cache, local_lqp_cache);

  // No caches
  const auto sql_pipeline_3 =
      SQLPipelineBuilder{"SELECT * FROM table_a"}.with_pqp_cache(nullptr).with_lqp_cache(nullptr).create_pipeline();
  EXPECT_FALSE(sql_pipeline_3.pqp_cache);
  EXPECT_FALSE(sql_pipeline_3.lqp_cache);
}

TEST_F(SQLPipelineTest, PrecheckDDLOperators) {
  auto sql_pipeline_1 = SQLPipelineBuilder{"CREATE TABLE t (a_int INTEGER)"}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline_1.get_result_table());

  auto sql_pipeline_2 = SQLPipelineBuilder{"CREATE TABLE t (a_int INTEGER)"}.create_pipeline();
  EXPECT_THROW(sql_pipeline_2.get_result_table(), InvalidInputException);

  auto sql_pipeline_3 = SQLPipelineBuilder{"DROP TABLE t"}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline_3.get_result_table());

  auto sql_pipeline_4 = SQLPipelineBuilder{"DROP TABLE t"}.create_pipeline();
  EXPECT_THROW(sql_pipeline_4.get_result_table(), InvalidInputException);

  auto sql_pipeline_5 = SQLPipelineBuilder{"DROP TABLE IF EXISTS t"}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline_5.get_result_table());

  auto sql_pipeline_6 = SQLPipelineBuilder{"CREATE TABLE t2 (a_int INTEGER); DROP TABLE t2"}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline_6.get_result_table());
}

TEST_F(SQLPipelineTest, GetResultTableNoReexecuteOnConflict) {
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  EXPECT_EQ(_table_a->row_count(), 3);

  {
    const auto conflicting_sql = "UPDATE table_a SET a = 100 WHERE b < 457";
    auto conflicting_sql_pipeline = SQLPipelineBuilder{conflicting_sql}.create_pipeline();
    (void)conflicting_sql_pipeline.get_result_table();
  }

  // The UPDATE should have inserted a new version of that row
  EXPECT_EQ(_table_a->row_count(), 4);

  const auto sql = "INSERT INTO table_a (a, b) VALUES (1, 2.0); UPDATE table_a SET a = a + 1 WHERE b < 457";
  auto sql_pipeline = SQLPipelineBuilder{sql}.with_transaction_context(transaction_context).create_pipeline();
  const auto [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Failure);
  EXPECT_EQ(table, nullptr);

  // The INSERT could not be committed, but still created a row that never became fully visible
  EXPECT_EQ(_table_a->row_count(), 5);

  const auto verify_table_contents = []() {
    const auto verification_sql = "SELECT a FROM table_a WHERE b < 457";
    auto verification_pipeline = SQLPipelineBuilder{verification_sql}.create_pipeline();
    const auto [verification_status, verification_table] = verification_pipeline.get_result_table();
    EXPECT_EQ(verification_status, SQLPipelineStatus::Success);
    EXPECT_EQ(verification_table->get_value<int32_t>("a", 0), 100);
  };
  verify_table_contents();

  // Check that this doesn't crash. This should not modify the table a second time.
  const auto [pipeline_status2, table2] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status2, SQLPipelineStatus::Failure);
  EXPECT_EQ(table2, nullptr);
  verify_table_contents();

  // The INSERT should not have been executed a second time
  EXPECT_EQ(_table_a->row_count(), 5);
}

}  // namespace hyrise
