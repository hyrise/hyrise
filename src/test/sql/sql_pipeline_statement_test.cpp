#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"

namespace {
// This function is a slightly hacky way to check whether an LQP was optimized. This relies on JoinOrderingRule and
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
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    _table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_b", _table_b);

    _table_int = load_table("resources/test_data/tbl/int_int_int.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_int", _table_int);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, false);
    column_definitions.emplace_back("bb", DataType::Float, false);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);

    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});

    _int_float_column_definitions.emplace_back("a", DataType::Int, false);
    _int_float_column_definitions.emplace_back("b", DataType::Float, false);

    _int_int_int_column_definitions.emplace_back("a", DataType::Int, false);
    _int_int_int_column_definitions.emplace_back("b", DataType::Int, false);
    _int_int_int_column_definitions.emplace_back("c", DataType::Int, false);

    _select_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_select_query_a, _select_parse_result.get());

    _multi_statement_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_multi_statement_dependant, _multi_statement_parse_result.get());

    _lqp_cache = std::make_shared<SQLLogicalPlanCache>();
    _pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  }

  // Access via friendship
  const std::vector<std::shared_ptr<SQLPipelineStatement>>& get_sql_pipeline_statements(SQLPipeline& sql_pipeline) {
    return sql_pipeline._get_sql_pipeline_statements();
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _table_int;
  std::shared_ptr<Table> _join_result;

  TableColumnDefinitions _int_float_column_definitions;
  TableColumnDefinitions _int_int_int_column_definitions;

  std::shared_ptr<SQLLogicalPlanCache> _lqp_cache;
  std::shared_ptr<SQLPhysicalPlanCache> _pqp_cache;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
  const std::string _multi_statement_query = "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  const std::string _multi_statement_dependant = "CREATE VIEW foo AS SELECT * FROM table_a; SELECT * FROM foo;";

  std::shared_ptr<hsql::SQLParserResult> _select_parse_result;
  std::shared_ptr<hsql::SQLParserResult> _multi_statement_parse_result;

  static bool _contains_validate(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
    for (const auto& task : tasks) {
      if (auto op_task = std::dynamic_pointer_cast<OperatorTask>(task)) {
        if (std::dynamic_pointer_cast<Validate>(op_task->get_operator())) return true;
      }
    }
    return false;
  }
};

TEST_F(SQLPipelineStatementTest, SimpleCreation) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_EQ(statement->get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_EQ(statement->get_sql_string(), _join_query);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithCustomTransactionContext) {
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline();

  // Execute SQLPipeline so that the transaction context gets set in the statement(s)
  (void)sql_pipeline.get_result_tables();

  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  EXPECT_EQ(statement->transaction_context(), context);
  EXPECT_EQ(statement->get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options

  const auto optimizer = Optimizer::create_default_optimizer();
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // No transaction context
  auto sql_pipeline1 = SQLPipelineBuilder{_select_query_a}.with_optimizer(optimizer).create_pipeline();
  auto statement1 = get_sql_pipeline_statements(sql_pipeline1).at(0);
  EXPECT_EQ(statement1->transaction_context(), nullptr);
  EXPECT_EQ(statement1->get_sql_string(), _select_query_a);

  auto sql_pipeline2 = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement2 = get_sql_pipeline_statements(sql_pipeline2).at(0);
  EXPECT_EQ(statement2->transaction_context(), nullptr);
  EXPECT_EQ(statement2->get_sql_string(), _select_query_a);

  auto sql_pipeline3 = SQLPipelineBuilder{_select_query_a}.with_optimizer(optimizer).create_pipeline();
  auto statement3 = get_sql_pipeline_statements(sql_pipeline3).at(0);
  EXPECT_EQ(statement3->transaction_context(), nullptr);
  EXPECT_EQ(statement3->get_sql_string(), _select_query_a);

  // With transaction context
  auto sql_pipeline4 = SQLPipelineBuilder{_select_query_a}
                           .with_optimizer(optimizer)
                           .with_transaction_context(transaction_context)
                           .create_pipeline();
  // Execute SQLPipeline so that the transaction context gets set in the statement(s)
  (void)sql_pipeline4.get_result_tables();
  auto statement4 = get_sql_pipeline_statements(sql_pipeline4).at(0);
  EXPECT_EQ(statement4->transaction_context(), transaction_context);
  EXPECT_EQ(statement4->get_sql_string(), _select_query_a);

  auto sql_pipeline5 =
      SQLPipelineBuilder{_select_query_a}.with_transaction_context(transaction_context).create_pipeline();
  (void)sql_pipeline5.get_result_tables();
  auto statement5 = get_sql_pipeline_statements(sql_pipeline5).at(0);
  EXPECT_EQ(statement5->transaction_context(), transaction_context);
  EXPECT_EQ(statement5->get_sql_string(), _select_query_a);

  auto sql_pipeline6 = SQLPipelineBuilder{_select_query_a}
                           .with_optimizer(optimizer)
                           .with_transaction_context(transaction_context)
                           .create_pipeline();
  (void)sql_pipeline6.get_result_tables();
  auto statement6 = get_sql_pipeline_statements(sql_pipeline6).at(0);
  EXPECT_EQ(statement6->transaction_context(), transaction_context);
  EXPECT_EQ(statement6->get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQL) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto& parsed_sql = statement->get_parsed_sql_statement();

  EXPECT_TRUE(parsed_sql->isValid());

  auto statements = parsed_sql->getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_unoptimized_logical_plan();

  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  statement->get_unoptimized_logical_plan();
  const auto& lqp = statement->get_unoptimized_logical_plan();

  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_TRUE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_FALSE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_optimized_logical_plan();

  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  statement->get_optimized_logical_plan();
  const auto& lqp = statement->get_optimized_logical_plan();

  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_TRUE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& lqp = statement->get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(statement->transaction_context(), nullptr);
  EXPECT_FALSE(lqp_is_validated(lqp));
}

TEST_F(SQLPipelineStatementTest, GetCachedOptimizedLQPValidated) {
  // Expect cache to be empty
  EXPECT_FALSE(_lqp_cache->has(_select_query_a));

  auto validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_lqp_cache(_lqp_cache).create_pipeline();
  auto& validated_statement = get_sql_pipeline_statements(validated_sql_pipeline).at(0);

  const auto& validated_lqp = validated_statement->get_optimized_logical_plan();
  EXPECT_TRUE(lqp_is_validated(validated_lqp));

  // Expect cache to contain validated LQP
  EXPECT_TRUE(_lqp_cache->has(_select_query_a));
  const auto validated_cached_lqp = _lqp_cache->try_get(_select_query_a);
  EXPECT_TRUE(lqp_is_validated(*validated_cached_lqp));

  // Evict validated version by requesting a not validated version
  auto not_validated_sql_pipeline =
      SQLPipelineBuilder{_select_query_a}.with_lqp_cache(_lqp_cache).disable_mvcc().create_pipeline();
  auto& not_validated_statement = get_sql_pipeline_statements(not_validated_sql_pipeline).at(0);
  const auto& not_validated_lqp = not_validated_statement->get_optimized_logical_plan();
  EXPECT_FALSE(lqp_is_validated(not_validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(_lqp_cache->has(_select_query_a));
  const auto not_validated_cached_lqp = _lqp_cache->try_get(_select_query_a);
  EXPECT_FALSE(lqp_is_validated(*not_validated_cached_lqp));
}

TEST_F(SQLPipelineStatementTest, GetCachedOptimizedLQPNotValidated) {
  // Expect cache to be empty
  EXPECT_FALSE(_lqp_cache->has(_select_query_a));

  auto not_validated_sql_pipeline =
      SQLPipelineBuilder{_select_query_a}.with_lqp_cache(_lqp_cache).disable_mvcc().create_pipeline();
  auto& not_validated_statement = get_sql_pipeline_statements(not_validated_sql_pipeline).at(0);

  const auto& not_validated_lqp = not_validated_statement->get_optimized_logical_plan();
  EXPECT_FALSE(lqp_is_validated(not_validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(_lqp_cache->has(_select_query_a));
  const auto not_validated_cached_lqp = _lqp_cache->try_get(_select_query_a);
  EXPECT_FALSE(lqp_is_validated(*not_validated_cached_lqp));

  // Evict not validated version by requesting a validated version
  auto validated_sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_lqp_cache(_lqp_cache).create_pipeline();
  auto& validated_statement = get_sql_pipeline_statements(validated_sql_pipeline).at(0);
  const auto& validated_lqp = validated_statement->get_optimized_logical_plan();
  EXPECT_TRUE(lqp_is_validated(validated_lqp));

  // Expect cache to contain not validated LQP
  EXPECT_TRUE(_lqp_cache->has(_select_query_a));
  const auto validated_cached_lqp = _lqp_cache->try_get(_select_query_a);
  EXPECT_TRUE(lqp_is_validated(*validated_cached_lqp));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPDoesNotInfluenceUnoptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& unoptimized_lqp = statement->get_unoptimized_logical_plan();

  // The optimizer works on the original LQP nodes which could be modified during optimization.
  // Copy the structure to check that it is equal after optimizing.
  std::shared_ptr<AbstractLQPNode> unoptimized_copy = unoptimized_lqp->deep_copy();

  // Optimize the LQP node
  statement->get_optimized_logical_plan();
  const auto& unoptimized_lqp_new = statement->get_unoptimized_logical_plan();

  EXPECT_LQP_EQ(unoptimized_copy, unoptimized_lqp_new);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlan) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  // We don't have a transaction context yet, as it was not needed
  EXPECT_EQ(statement->transaction_context(), nullptr);

  const auto& plan = statement->get_physical_plan();
  EXPECT_NE(plan, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  statement->get_physical_plan();
  auto duration = statement->metrics()->lqp_translation_duration;

  const auto& plan = statement->get_physical_plan();
  auto duration2 = statement->metrics()->lqp_translation_duration;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  EXPECT_NE(plan, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanJoinWithFilter) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& plan = statement->get_physical_plan();

  auto is_join_op = [](const std::shared_ptr<const AbstractOperator>& node) {
    return static_cast<bool>(std::dynamic_pointer_cast<const AbstractJoinOperator>(node));
  };

  EXPECT_NE(plan, nullptr);
  EXPECT_TRUE(contained_in_query_plan(plan, is_join_op));
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto& plan = statement->get_physical_plan();

  EXPECT_NE(plan->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto& plan = statement->get_physical_plan();

  EXPECT_EQ(plan->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithCustomTransactionContext) {
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline();

  (void)sql_pipeline.get_result_tables();

  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto& plan = statement->get_physical_plan();

  EXPECT_EQ(plan->transaction_context(), context);
}

TEST_F(SQLPipelineStatementTest, GetTasks) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& tasks = statement->get_tasks();

  //  [0] [Validate]
  //  \_[1] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  statement->get_tasks();
  const auto& tasks = statement->get_tasks();

  //  [0] [Validate]
  //  \_[1] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksNotValidated) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& tasks = statement->get_tasks();

  // [0] [StoredTable] Name: 'table_a'
  EXPECT_EQ(tasks.size(), 1u);
  EXPECT_FALSE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetResultTable) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto [pipeline_status, table] = statement->get_result_table();

  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineStatementTest, GetResultTableTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  statement->get_result_table();
  auto duration = statement->metrics()->plan_execution_duration;

  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  auto duration2 = statement->metrics()->plan_execution_duration;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineStatementTest, GetResultTableJoin) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoOutputNoReexecution) {
  const auto sql = "UPDATE table_a SET a = a + 1 WHERE b < 457";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  EXPECT_EQ(table, nullptr);

  const auto verify_table_contents = [this]() {
    const auto verification_sql = "SELECT a FROM table_a WHERE b < 457";
    auto verification_pipeline = SQLPipelineBuilder{verification_sql}.create_pipeline();
    auto& verification_statement = get_sql_pipeline_statements(verification_pipeline).at(0);
    const auto [verification_status, verification_table] = verification_statement->get_result_table();
    EXPECT_EQ(verification_status, SQLPipelineStatus::Success);
    EXPECT_EQ(verification_table->get_value<int32_t>("a", 0), 124);
  };
  verify_table_contents();

  // Check that this doesn't crash. This should not modify the table a second time.
  const auto [pipeline_status2, table2] = statement->get_result_table();
  EXPECT_EQ(pipeline_status2, SQLPipelineStatus::Success);
  EXPECT_EQ(table2, nullptr);
  verify_table_contents();
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.disable_mvcc().create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);

  // Check that there really is no transaction management
  EXPECT_EQ(statement->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetResultTableTransactionFailureExplicitTransaction) {
  // Mark a row as modified by a different transaction
  _table_a->get_chunk(ChunkID{0})->mvcc_data()->set_tid(0, TransactionID{17});

  const auto sql = "UPDATE table_a SET a = 1";
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  statement->set_transaction_context(transaction_context);

  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Failure);
  EXPECT_EQ(table, nullptr);

  // Retrieving it again should give us the same result
  const auto [pipeline_status2, table2] = statement->get_result_table();
  EXPECT_EQ(pipeline_status2, SQLPipelineStatus::Failure);
  EXPECT_EQ(table2, nullptr);

  EXPECT_TRUE(transaction_context->aborted());
}

TEST_F(SQLPipelineStatementTest, GetResultTableTransactionFailureAutoCommit) {
  // Mark a row as modified by a different transaction
  _table_a->get_chunk(ChunkID{0})->mvcc_data()->set_tid(0, TransactionID{17});

  const auto sql = "UPDATE table_a SET a = 1";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto [pipeline_status, table] = statement->get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Failure);
  EXPECT_EQ(table, nullptr);

  // Retrieving it again should give us the same result
  const auto [pipeline_status2, table2] = statement->get_result_table();
  EXPECT_EQ(pipeline_status2, SQLPipelineStatus::Failure);
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetTimes) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);

  const auto& metrics = statement->metrics();
  const auto zero_duration = std::chrono::nanoseconds::zero();

  EXPECT_EQ(metrics->sql_translation_duration, zero_duration);
  EXPECT_EQ(metrics->optimization_duration, zero_duration);
  EXPECT_EQ(metrics->lqp_translation_duration, zero_duration);
  EXPECT_EQ(metrics->plan_execution_duration, zero_duration);

  // Run to get times
  statement->get_result_table();

  EXPECT_GT(metrics->sql_translation_duration, zero_duration);
  EXPECT_GT(metrics->optimization_duration, zero_duration);
  EXPECT_GT(metrics->lqp_translation_duration, zero_duration);
  EXPECT_GT(metrics->plan_execution_duration, zero_duration);
}

TEST_F(SQLPipelineStatementTest, CacheQueryPlan) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_lqp_cache(_lqp_cache).create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  statement->get_result_table();

  EXPECT_EQ(_lqp_cache->size(), 1u);
  EXPECT_TRUE(_lqp_cache->has(_select_query_a));
}

TEST_F(SQLPipelineStatementTest, CopySubselectFromCache) {
  const auto subquery_query = "SELECT * FROM table_int WHERE a = (SELECT MAX(b) FROM table_int)";

  auto first_subquery_sql_pipeline = SQLPipelineBuilder{subquery_query}.create_pipeline();
  auto& first_subquery_statement = get_sql_pipeline_statements(first_subquery_sql_pipeline).at(0);

  const auto [first_subquery_status, first_subquery_result] = first_subquery_statement->get_result_table();
  EXPECT_EQ(first_subquery_status, SQLPipelineStatus::Success);

  auto expected_first_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_first_result->append({10, 10, 10});

  EXPECT_TABLE_EQ_UNORDERED(first_subquery_result, expected_first_result);

  SQLPipelineBuilder{"INSERT INTO table_int VALUES (11, 11, 11)"}.create_pipeline().get_result_table();

  auto second_subquery_sql_pipeline = SQLPipelineBuilder{subquery_query}.create_pipeline();
  auto& second_subquery_statement = get_sql_pipeline_statements(second_subquery_sql_pipeline).at(0);
  const auto [second_subquery_status, second_subquery_result] = second_subquery_statement->get_result_table();
  EXPECT_EQ(second_subquery_status, SQLPipelineStatus::Success);

  auto expected_second_result = std::make_shared<Table>(_int_int_int_column_definitions, TableType::Data);
  expected_second_result->append({11, 10, 11});
  expected_second_result->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(second_subquery_result, expected_second_result);
}

TEST_F(SQLPipelineStatementTest, DefaultPlanCaches) {
  const auto default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  const auto local_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  const auto default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  const auto local_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  // No caches
  auto sql_pipeline_0 = SQLPipelineBuilder{"SELECT * FROM table_a"}.create_pipeline();
  auto statement_0 = get_sql_pipeline_statements(sql_pipeline_0).at(0);
  EXPECT_FALSE(statement_0->pqp_cache);
  EXPECT_FALSE(statement_0->lqp_cache);

  // Default caches
  Hyrise::get().default_pqp_cache = default_pqp_cache;
  Hyrise::get().default_lqp_cache = default_lqp_cache;
  auto sql_pipeline_1 = SQLPipelineBuilder{"SELECT * FROM table_a"}.create_pipeline();
  auto statement_1 = get_sql_pipeline_statements(sql_pipeline_1).at(0);
  EXPECT_EQ(statement_1->pqp_cache, default_pqp_cache);
  EXPECT_EQ(statement_1->lqp_cache, default_lqp_cache);

  // Local caches
  auto sql_pipeline_2 = SQLPipelineBuilder{"SELECT * FROM table_a"}
                            .with_pqp_cache(local_pqp_cache)
                            .with_lqp_cache(local_lqp_cache)
                            .create_pipeline();
  auto statement_2 = get_sql_pipeline_statements(sql_pipeline_2).at(0);
  EXPECT_EQ(statement_2->pqp_cache, local_pqp_cache);
  EXPECT_EQ(statement_2->lqp_cache, local_lqp_cache);

  // No caches
  auto sql_pipeline_3 =
      SQLPipelineBuilder{"SELECT * FROM table_a"}.with_pqp_cache(nullptr).with_lqp_cache(nullptr).create_pipeline();
  auto statement_3 = get_sql_pipeline_statements(sql_pipeline_3).at(0);
  EXPECT_FALSE(statement_3->pqp_cache);
  EXPECT_FALSE(statement_3->lqp_cache);
}

TEST_F(SQLPipelineStatementTest, MetaTableNoCaching) {
  const auto meta_table_query = "SELECT * FROM " + MetaTableManager::META_PREFIX + "tables";

  auto sql_pipeline =
      SQLPipelineBuilder{meta_table_query}.with_lqp_cache(_lqp_cache).with_pqp_cache(_pqp_cache).create_pipeline();
  auto statement = get_sql_pipeline_statements(sql_pipeline).at(0);
  statement->get_result_table();

  EXPECT_EQ(_lqp_cache->size(), 0u);
  EXPECT_FALSE(_lqp_cache->has(meta_table_query));

  EXPECT_EQ(_pqp_cache->size(), 0u);
  EXPECT_FALSE(_pqp_cache->has(meta_table_query));
}

TEST_F(SQLPipelineStatementTest, SQLTranslationInfo) {
  {
    auto sql_pipeline = SQLPipelineBuilder{"SELECT * FROM table_a"}.create_pipeline();
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    EXPECT_TRUE(translation_info.cacheable);
    EXPECT_TRUE(translation_info.parameter_ids_of_value_placeholders.empty());
  }

  {
    auto sql_pipeline = SQLPipelineBuilder{"SELECT * FROM meta_tables"}.create_pipeline();
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    EXPECT_FALSE(translation_info.cacheable);
    EXPECT_TRUE(translation_info.parameter_ids_of_value_placeholders.empty());
  }

  {
    auto sql_pipeline = SQLPipelineBuilder{"SELECT * FROM table_a WHERE a > ? AND b BETWEEN 5 AND ?"}.create_pipeline();
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    EXPECT_TRUE(translation_info.cacheable);
    const auto parameters = std::vector<ParameterID>{ParameterID(0), ParameterID(1)};
    EXPECT_EQ(translation_info.parameter_ids_of_value_placeholders, parameters);
  }

  {
    auto sql_pipeline =
        SQLPipelineBuilder{
            "SELECT * FROM table_a t1 WHERE a > ? AND b = (SELECT MAX(b) FROM table_a t2 WHERE t2.a = t1.a AND b > ?)"}
            .create_pipeline();
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    EXPECT_TRUE(translation_info.cacheable);
    const auto parameters = std::vector<ParameterID>{ParameterID(0), ParameterID(2)};
    EXPECT_EQ(translation_info.parameter_ids_of_value_placeholders, parameters);
  }
}

}  // namespace opossum
