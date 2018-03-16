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

    _table_int = load_table("src/test/tables/int_int_int.tbl", 2);
    StorageManager::get().add_table("table_int", _table_int);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Float);
    column_definitions.emplace_back("bb", DataType::Float);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);

    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});

    _int_float_column_definitions.emplace_back("a", DataType::Int);
    _int_float_column_definitions.emplace_back("b", DataType::Float);

    _int_int_int_column_definitions.emplace_back("a", DataType::Int);
    _int_int_int_column_definitions.emplace_back("b", DataType::Int);
    _int_int_int_column_definitions.emplace_back("c", DataType::Int);

    _select_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_select_query_a, _select_parse_result.get());

    _multi_statement_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_multi_statement_dependant, _multi_statement_parse_result.get());

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _table_int;
  std::shared_ptr<Table> _join_result;

  TableColumnDefinitions _int_float_column_definitions;
  TableColumnDefinitions _int_int_int_column_definitions;

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
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithoutMVCC) {
  SQLPipelineStatement sql_pipeline{_join_query, UseMvcc::No};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_sql_string(), _join_query);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipelineStatement sql_pipeline{_select_query_a, context};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreation) {
  SQLPipelineStatement sql_pipeline{
      _select_query_a, _select_parse_result, UseMvcc::Yes, nullptr, Optimizer::create_default_optimizer(), nullptr};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithoutMVCC) {
  SQLPipelineStatement sql_pipeline{
      _select_query_a, _select_parse_result, UseMvcc::No, nullptr, Optimizer::create_default_optimizer(), nullptr};

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  SQLPipelineStatement sql_pipeline{
      _select_query_a, _select_parse_result, UseMvcc::Yes, context, Optimizer::create_default_optimizer(), nullptr};

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationTooManyStatements) {
  EXPECT_THROW(SQLPipelineStatement(_multi_statement_dependant, _multi_statement_parse_result, UseMvcc::No, nullptr,
                                    Optimizer::create_default_optimizer(), nullptr),
               std::exception);
}

TEST_F(SQLPipelineStatementTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options

  const auto optimizer = Optimizer::create_default_optimizer();
  auto prepared_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // No transaction context
  SQLPipelineStatement sql_pipeline1{_select_query_a, optimizer, UseMvcc::Yes};
  EXPECT_EQ(sql_pipeline1.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline1.get_sql_string(), _select_query_a);

  SQLPipelineStatement sql_pipeline2{_select_query_a, prepared_cache, UseMvcc::No};
  EXPECT_EQ(sql_pipeline2.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline2.get_sql_string(), _select_query_a);

  SQLPipelineStatement sql_pipeline3{_select_query_a, optimizer, prepared_cache, UseMvcc::Yes};
  EXPECT_EQ(sql_pipeline3.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline3.get_sql_string(), _select_query_a);

  // With transaction context
  SQLPipelineStatement sql_pipeline4{_select_query_a, optimizer, transaction_context};
  EXPECT_EQ(sql_pipeline4.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline4.get_sql_string(), _select_query_a);

  SQLPipelineStatement sql_pipeline5{_select_query_a, prepared_cache, transaction_context};
  EXPECT_EQ(sql_pipeline5.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline5.get_sql_string(), _select_query_a);

  SQLPipelineStatement sql_pipeline6{_select_query_a, optimizer, prepared_cache, transaction_context};
  EXPECT_EQ(sql_pipeline6.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline6.get_sql_string(), _select_query_a);
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
  EXPECT_TRUE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPNotValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a, UseMvcc::No};

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subplan_is_validated());
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
  EXPECT_TRUE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPNotValidated) {
  SQLPipelineStatement sql_pipeline{_select_query_a, UseMvcc::No};

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subplan_is_validated());
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

  EXPECT_LQP_EQ(unoptimized_copy, unoptimized_lqp_new);
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
  SQLPipelineStatement sql_pipeline{_select_query_a, UseMvcc::No};
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
  SQLPipelineStatement sql_pipeline{_select_query_a, UseMvcc::No};

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
  SQLPipelineStatement sql_pipeline{sql, UseMvcc::No};

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
  SQLPipelineStatement sql_pipeline{_select_query_a, UseMvcc::No};

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

TEST_F(SQLPipelineStatementTest, PreparedStatementPrepare) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  SQLPipelineStatement sql_pipeline{prepared_statement, prepared_statement_cache};

  sql_pipeline.get_query_plan();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);
  EXPECT_TRUE(prepared_statement_cache->has("x1"));

  EXPECT_NO_THROW(sql_pipeline.get_result_table());
}

TEST_F(SQLPipelineStatementTest, PreparedStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  SQLPipelineStatement prepare_sql_pipeline{prepared_statement, prepared_statement_cache};
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123)";
  SQLPipelineStatement execute_sql_pipeline{execute_statement, prepared_statement_cache};
  const auto& table = execute_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({123, 456.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, PreparedStatementMultiPlaceholderExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ? OR (a > ? AND b < ?)'";
  SQLPipelineStatement prepare_sql_pipeline{prepared_statement, prepared_statement_cache};
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123, 10000, 500)";
  SQLPipelineStatement execute_sql_pipeline{execute_statement, prepared_statement_cache};
  const auto& table = execute_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({123, 456.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, MultiplePreparedStatementsExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement1 = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  const std::string prepared_statement2 = "PREPARE x2 FROM 'SELECT * FROM table_a WHERE a > ?'";
  const std::string prepared_statement_multi =
      "PREPARE x_multi FROM 'SELECT * FROM table_a WHERE a = ? OR (a > ? AND b < ?)'";

  SQLPipelineStatement prepare_sql_pipeline1{prepared_statement1, prepared_statement_cache};
  SQLPipelineStatement prepare_sql_pipeline2{prepared_statement2, prepared_statement_cache};
  SQLPipelineStatement prepare_sql_pipeline_multi{prepared_statement_multi, prepared_statement_cache};

  prepare_sql_pipeline1.get_result_table();
  prepare_sql_pipeline2.get_result_table();
  prepare_sql_pipeline_multi.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 3u);
  EXPECT_TRUE(prepared_statement_cache->has("x1"));
  EXPECT_TRUE(prepared_statement_cache->has("x2"));
  EXPECT_TRUE(prepared_statement_cache->has("x_multi"));

  const std::string execute_statement1 = "EXECUTE x1 (123)";
  const std::string execute_statement1_invalid = "EXECUTE x1 (123, 10000)";  // too many arguments

  const std::string execute_statement2 = "EXECUTE x2 (10000)";
  const std::string execute_statement2_invalid = "EXECUTE x2";  // too few arguments

  const std::string execute_statement_multi = "EXECUTE x_multi (123, 10000, 500)";
  const std::string execute_statement_multi_invalid = "EXECUTE x_multi (123, 10000, 500, 100)";  // too many arguments

  EXPECT_THROW(SQLPipelineStatement(execute_statement1_invalid).get_result_table(), std::runtime_error);
  EXPECT_THROW(SQLPipelineStatement(execute_statement2_invalid).get_result_table(), std::runtime_error);
  EXPECT_THROW(SQLPipelineStatement(execute_statement_multi_invalid).get_result_table(), std::runtime_error);

  SQLPipelineStatement execute_sql_pipeline1{execute_statement1, prepared_statement_cache};
  const auto& table1 = execute_sql_pipeline1.get_result_table();

  SQLPipelineStatement execute_sql_pipeline2{execute_statement2, prepared_statement_cache};
  const auto& table2 = execute_sql_pipeline2.get_result_table();

  SQLPipelineStatement execute_sql_pipeline_multi{execute_statement_multi, prepared_statement_cache};
  const auto& table_multi = execute_sql_pipeline_multi.get_result_table();

  // x1 result
  auto expected1 = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected1->append({123, 456.7f});

  // x2 result
  auto expected2 = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected2->append({12345, 458.7f});

  // x_multi result
  auto expected_multi = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected_multi->append({123, 456.7f});
  expected_multi->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table1, expected1);
  EXPECT_TABLE_EQ_UNORDERED(table2, expected2);
  EXPECT_TABLE_EQ_UNORDERED(table_multi, expected_multi);
}

TEST_F(SQLPipelineStatementTest, PreparedInsertStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'INSERT INTO table_a VALUES (?, ?)'";
  SQLPipelineStatement prepare_sql_pipeline{prepared_statement, prepared_statement_cache};
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (1, 0.75)";
  SQLPipelineStatement execute_sql_pipeline{execute_statement, prepared_statement_cache};
  execute_sql_pipeline.get_result_table();

  SQLPipelineStatement select_sql_pipeline{_select_query_a};
  const auto table = select_sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineStatementTest, PreparedUpdateStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'UPDATE table_a SET a = ? WHERE a = ?'";
  SQLPipelineStatement prepare_sql_pipeline{prepared_statement, prepared_statement_cache};
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (1, 123)";
  SQLPipelineStatement execute_sql_pipeline{execute_statement, prepared_statement_cache};
  execute_sql_pipeline.get_result_table();

  SQLPipelineStatement select_sql_pipeline{_select_query_a};
  const auto table = select_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({1, 456.7f});
  expected->append({1234, 457.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, PreparedDeleteStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'DELETE FROM table_a WHERE a = ?'";
  SQLPipelineStatement prepare_sql_pipeline{prepared_statement, prepared_statement_cache};
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123)";
  SQLPipelineStatement execute_sql_pipeline{execute_statement, prepared_statement_cache};
  execute_sql_pipeline.get_result_table();

  SQLPipelineStatement select_sql_pipeline{_select_query_a};
  const auto table = select_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({1234, 457.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, CacheQueryPlan) {
  SQLPipelineStatement sql_pipeline{_select_query_a};
  sql_pipeline.get_result_table();

  const auto& cache = SQLQueryCache<SQLQueryPlan>::get();
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_TRUE(cache.has(_select_query_a));
}

TEST_F(SQLPipelineStatementTest, RecreateSubselectFromCache) {
  const std::string subselect_query = "SELECT * FROM table_int WHERE a = (SELECT MAX(b) FROM table_int)";

  SQLPipelineStatement first_subselect_sql_pipeline{subselect_query};
  const auto first_subselect_result = first_subselect_sql_pipeline.get_result_table();

  auto expected_first_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_first_result->append({10, 10, 10});

  EXPECT_TABLE_EQ_UNORDERED(first_subselect_result, expected_first_result);

  SQLPipelineStatement{"INSERT INTO table_int VALUES (11, 11, 11)"}.get_result_table();

  SQLPipelineStatement second_subselect_sql_pipeline{subselect_query};
  const auto second_subselect_result = second_subselect_sql_pipeline.get_result_table();

  auto expected_second_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_second_result->append({11, 10, 11});
  expected_second_result->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(second_subselect_result, expected_second_result);
}

}  // namespace opossum
