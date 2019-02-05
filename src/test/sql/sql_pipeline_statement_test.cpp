#include <memory>
#include <string>
#include <utility>
#include "base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "gtest/gtest.h"

#include "cache/cache.hpp"
#include "logical_query_plan/join_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/storage_manager.hpp"

namespace {
// This function is a slightly hacky way to check whether an LQP was optimized. This relies on JoinDetectionRule and
// could break if something is changed within the optimizer.
// It assumes that for the query: SELECT * from a, b WHERE a.a = b.a will be translated to a Cross Join with a filter
// predicate and then optimized to a Join.
std::function<bool(const std::shared_ptr<opossum::AbstractLQPNode>&)> contains_cross =
    [](const std::shared_ptr<opossum::AbstractLQPNode>& node) {
      if (node->type != opossum::LQPNodeType::Join) return false;
      if (auto join_node = std::dynamic_pointer_cast<opossum::JoinNode>(node)) {
        return join_node->join_mode == opossum::JoinMode::Cross;
      }
      return false;
    };
}  // namespace

namespace opossum {

class SQLPipelineStatementTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    _table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _table_b);

    _table_int = load_table("resources/test_data/tbl/int_int_int.tbl", 2);
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

    SQLPhysicalPlanCache::get().clear();
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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.disable_mvcc().create_pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_sql_string(), _join_query);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreation) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement(_select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithoutMVCC) {
  auto sql_pipeline =
      SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement(_select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline_statement(
      _select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationTooManyStatements) {
  EXPECT_THROW(SQLPipelineBuilder(_multi_statement_dependant)
                   .disable_mvcc()
                   .create_pipeline_statement(_multi_statement_parse_result),
               std::exception);
}

TEST_F(SQLPipelineStatementTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options

  const auto optimizer = Optimizer::create_default_optimizer();
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // No transaction context
  auto sql_pipeline1 = SQLPipelineBuilder{_select_query_a}.with_optimizer(optimizer).create_pipeline_statement();
  EXPECT_EQ(sql_pipeline1.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline1.get_sql_string(), _select_query_a);

  auto sql_pipeline2 = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();
  EXPECT_EQ(sql_pipeline2.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline2.get_sql_string(), _select_query_a);

  auto sql_pipeline3 = SQLPipelineBuilder{_select_query_a}.with_optimizer(optimizer).create_pipeline_statement();
  EXPECT_EQ(sql_pipeline3.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline3.get_sql_string(), _select_query_a);

  // With transaction context
  auto sql_pipeline4 = SQLPipelineBuilder{_select_query_a}
                           .with_optimizer(optimizer)
                           .with_transaction_context(transaction_context)
                           .create_pipeline_statement();
  EXPECT_EQ(sql_pipeline4.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline4.get_sql_string(), _select_query_a);

  auto sql_pipeline5 =
      SQLPipelineBuilder{_select_query_a}.with_transaction_context(transaction_context).create_pipeline_statement();
  EXPECT_EQ(sql_pipeline5.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline5.get_sql_string(), _select_query_a);

  auto sql_pipeline6 = SQLPipelineBuilder{_select_query_a}
                           .with_optimizer(optimizer)
                           .with_transaction_context(transaction_context)
                           .create_pipeline_statement();
  EXPECT_EQ(sql_pipeline6.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline6.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQL) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();
  const auto& parsed_sql = sql_pipeline.get_parsed_sql_statement();

  EXPECT_TRUE(parsed_sql->isValid());

  auto statements = parsed_sql->getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQLMulti) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline_statement();
  EXPECT_THROW(sql_pipeline.get_parsed_sql_statement(), std::exception);
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  sql_pipeline.get_unoptimized_logical_plan();
  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  sql_pipeline.get_optimized_logical_plan();
  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetCachedOptimizedLQPValidated) {
  // Expect cache to be empty
  EXPECT_FALSE(SQLLogicalPlanCache::get().has(_select_query_a));

  auto validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  const auto& validated_lqp = validated_sql_pipeline.get_optimized_logical_plan();
  EXPECT_TRUE(lqp_is_validated(validated_lqp));

  // Expect cache to contain validated LQP
  EXPECT_TRUE(SQLLogicalPlanCache::get().has(_select_query_a));
  const auto validated_cached_lqp = SQLLogicalPlanCache::get().get_entry(_select_query_a);
  EXPECT_TRUE(lqp_is_validated(validated_cached_lqp));

  // Evict validated version by requesting a not validated version
  auto not_validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();
  const auto& not_validated_lqp = not_validated_sql_pipeline.get_optimized_logical_plan();
  EXPECT_FALSE(lqp_is_validated(not_validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(SQLLogicalPlanCache::get().has(_select_query_a));
  const auto not_validated_cached_lqp = SQLLogicalPlanCache::get().get_entry(_select_query_a);
  EXPECT_FALSE(lqp_is_validated(not_validated_cached_lqp));
}

TEST_F(SQLPipelineStatementTest, GetCachedOptimizedLQPNotValidated) {
  // Expect cache to be empty
  EXPECT_FALSE(SQLLogicalPlanCache::get().has(_select_query_a));

  auto not_validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();

  const auto& not_validated_lqp = not_validated_sql_pipeline.get_optimized_logical_plan();
  EXPECT_FALSE(lqp_is_validated(not_validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(SQLLogicalPlanCache::get().has(_select_query_a));
  const auto not_validated_cached_lqp = SQLLogicalPlanCache::get().get_entry(_select_query_a);
  EXPECT_FALSE(lqp_is_validated(not_validated_cached_lqp));

  // Evict not validated version by requesting a validated version
  auto validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();
  const auto& validated_lqp = validated_sql_pipeline.get_optimized_logical_plan();
  EXPECT_TRUE(lqp_is_validated(validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(SQLLogicalPlanCache::get().has(_select_query_a));
  const auto validated_cached_lqp = SQLLogicalPlanCache::get().get_entry(_select_query_a);
  EXPECT_TRUE(lqp_is_validated(validated_cached_lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPDoesNotInfluenceUnoptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

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
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  // We don't have a transaction context yet, as it was not needed
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);

  const auto& plan = sql_pipeline.get_physical_plan();
  EXPECT_NE(plan, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  sql_pipeline.get_physical_plan();
  auto duration = sql_pipeline.metrics()->lqp_translate_time_nanos;

  const auto& plan = sql_pipeline.get_physical_plan();
  auto duration2 = sql_pipeline.metrics()->lqp_translate_time_nanos;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  EXPECT_NE(plan, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanJoinWithFilter) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  const auto& plan = sql_pipeline.get_physical_plan();

  auto is_join_op = [](const std::shared_ptr<const AbstractOperator>& node) {
    return static_cast<bool>(std::dynamic_pointer_cast<const AbstractJoinOperator>(node));
  };

  EXPECT_NE(plan, nullptr);
  EXPECT_TRUE(contained_in_query_plan(plan, is_join_op));
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();
  const auto& plan = sql_pipeline.get_physical_plan();

  EXPECT_NE(plan->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();
  const auto& plan = sql_pipeline.get_physical_plan();

  EXPECT_EQ(plan->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline_statement();
  const auto& plan = sql_pipeline.get_physical_plan();

  EXPECT_EQ(plan->transaction_context().get(), context.get());
}

TEST_F(SQLPipelineStatementTest, GetTasks) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  const auto& tasks = sql_pipeline.get_tasks();

  //  [0] [Validate]
  //  \_[1] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  //  [0] [Validate]
  //  \_[1] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();

  const auto& tasks = sql_pipeline.get_tasks();

  // [0] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 1u);
  EXPECT_FALSE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetResultTable) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  sql_pipeline.get_result_table();
  auto duration = sql_pipeline.metrics()->execution_time_nanos;

  const auto& table = sql_pipeline.get_result_table();
  auto duration2 = sql_pipeline.metrics()->execution_time_nanos;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableJoin) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline_statement();

  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 5";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline_statement();

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table, otherwise the auto-commit will fail.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline_statement();

  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);

  // Check that there really is no transaction management
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetTimes) {
  const auto& cache = SQLPhysicalPlanCache::get();
  EXPECT_EQ(cache.size(), 0u);

  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();

  const auto& metrics = sql_pipeline.metrics();
  const auto zero_duration = std::chrono::nanoseconds::zero();

  EXPECT_EQ(metrics->sql_translate_time_nanos, zero_duration);
  EXPECT_EQ(metrics->optimize_time_nanos, zero_duration);
  EXPECT_EQ(metrics->lqp_translate_time_nanos, zero_duration);
  EXPECT_EQ(metrics->execution_time_nanos, zero_duration);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(metrics->sql_translate_time_nanos, zero_duration);
  EXPECT_GT(metrics->optimize_time_nanos, zero_duration);
  EXPECT_GT(metrics->lqp_translate_time_nanos, zero_duration);
  EXPECT_GT(metrics->execution_time_nanos, zero_duration);
}

TEST_F(SQLPipelineStatementTest, ParseErrorDebugMessage) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto sql_pipeline = SQLPipelineBuilder{_invalid_sql}.create_pipeline_statement();
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

TEST_F(SQLPipelineStatementTest, CacheQueryPlan) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline_statement();
  sql_pipeline.get_result_table();

  const auto& cache = SQLPhysicalPlanCache::get();
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_TRUE(cache.has(_select_query_a));
}

TEST_F(SQLPipelineStatementTest, CopySubselectFromCache) {
  const auto subquery_query = "SELECT * FROM table_int WHERE a = (SELECT MAX(b) FROM table_int)";

  auto first_subquery_sql_pipeline = SQLPipelineBuilder{subquery_query}.create_pipeline_statement();

  const auto first_subquery_result = first_subquery_sql_pipeline.get_result_table();

  auto expected_first_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_first_result->append({10, 10, 10});

  EXPECT_TABLE_EQ_UNORDERED(first_subquery_result, expected_first_result);

  SQLPipelineBuilder{"INSERT INTO table_int VALUES (11, 11, 11)"}.create_pipeline_statement().get_result_table();

  auto second_subquery_sql_pipeline = SQLPipelineBuilder{subquery_query}.create_pipeline_statement();
  const auto second_subquery_result = second_subquery_sql_pipeline.get_result_table();

  auto expected_second_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_second_result->append({11, 10, 11});
  expected_second_result->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(second_subquery_result, expected_second_result);
}

}  // namespace opossum
