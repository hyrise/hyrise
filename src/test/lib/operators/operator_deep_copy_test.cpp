#include <memory>
#include <utility>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/difference.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

// At the moment all the deep_copy() methods just call the constructor again. At first sight, these tests
// do not seem to add too much value because. This might change in the future. Then, these tests will
// make much more sense.

class OperatorDeepCopyTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    _table_b = load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2});
    _a_a = PQPColumnExpression::from_table(*_table_a, "a");
    _a_b = PQPColumnExpression::from_table(*_table_a, "b");
    _b_a = PQPColumnExpression::from_table(*_table_b, "a");
    _b_b = PQPColumnExpression::from_table(*_table_b, "b");

    Hyrise::get().storage_manager.add_table(_table_name_a, _table_a);
    Hyrise::get().storage_manager.add_table(_table_name_b, _table_b);

    _table_wrapper_a =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2}));
    _table_wrapper_b =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2}));
    _table_wrapper_c =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float3.tbl", ChunkOffset{2}));
    _table_wrapper_d =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int3.tbl", ChunkOffset{2}));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();
  }

  std::shared_ptr<PQPColumnExpression> _a_a, _a_b, _b_a, _b_b;
  std::shared_ptr<Table> _table_a, _table_b;
  std::string _table_name_a = "table_a";
  std::string _table_name_b = "table_b";
  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d;
};

template <typename T>
class DeepCopyTestJoin : public OperatorDeepCopyTest {};

// here we define all Join types
using JoinTypes = ::testing::Types<JoinNestedLoop, JoinHash, JoinSortMerge>;
TYPED_TEST_SUITE(DeepCopyTestJoin, JoinTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(DeepCopyTestJoin, DeepCopyJoin) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/join_operators/int_left_join_equals.tbl", ChunkOffset{1});
  EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

  // build and execute join
  auto join =
      std::make_shared<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b, JoinMode::Left,
                                  OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  EXPECT_NE(join, nullptr) << "Could not build Join";
  join->execute();
  EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);

  // Copy and execute copied join
  auto copied_join = join->deep_copy();
  EXPECT_NE(copied_join, nullptr) << "Could not copy Join";

  // table wrappers need to be executed manually
  copied_join->mutable_left_input()->execute();
  copied_join->mutable_right_input()->execute();
  copied_join->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_join->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyDifference) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_filtered2.tbl", ChunkOffset{2});

  // build and execute difference
  auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_c);
  difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(difference->get_output(), expected_result);

  // Copy and execute copies difference
  auto copied_difference = difference->deep_copy();
  EXPECT_NE(copied_difference, nullptr) << "Could not copy Difference";

  // table wrapper needs to be executed manually
  copied_difference->mutable_left_input()->execute();
  copied_difference->mutable_right_input()->execute();
  copied_difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_difference->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyPrint) {
  auto ostream = std::stringstream{};

  auto print1 = std::make_shared<Print>(_table_wrapper_a, PrintFlags::IgnoreChunkBoundaries, ostream);
  print1->execute();

  const auto result1 = ostream.str();

  // Reset ostream
  ostream.str("");
  ostream.clear();

  auto print2 = print1->deep_copy();
  print2->mutable_left_input()->execute();
  print2->execute();

  const auto result2 = ostream.str();

  EXPECT_EQ(result1, result2);
}

TEST_F(OperatorDeepCopyTest, DeepCopyGetTable) {
  // build and execute get table
  auto get_table = std::make_shared<GetTable>("table_a");
  get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(get_table->get_output(), _table_a);

  // copy end execute copied get table
  auto copied_get_table = get_table->deep_copy();
  EXPECT_NE(copied_get_table, nullptr) << "Could not copy GetTable";

  copied_get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_get_table->get_output(), _table_a);
}

TEST_F(OperatorDeepCopyTest, DeepCopyLimit) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_int3_limit_1.tbl", ChunkOffset{1});

  // build and execute limit
  auto limit = std::make_shared<Limit>(_table_wrapper_d, to_expression(int64_t{1}));
  limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(limit->get_output(), expected_result);

  // copy end execute copied limit
  auto copied_limit = limit->deep_copy();
  EXPECT_NE(copied_limit, nullptr) << "Could not copy Limit";

  // table wrapper needs to be executed manually
  copied_limit->mutable_left_input()->execute();
  copied_limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_limit->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopySort) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", ChunkOffset{1});

  // build and execute sort
  auto sort = std::make_shared<Sort>(
      _table_wrapper_a, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},
      ChunkOffset{2});
  sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(sort->get_output(), expected_result);

  // copy end execute copied sort
  auto copied_sort = sort->deep_copy();
  EXPECT_NE(copied_sort, nullptr) << "Could not copy Sort";

  // table wrapper needs to be executed manually
  copied_sort->mutable_left_input()->execute();
  copied_sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_sort->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyTableScan) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_filtered2.tbl", ChunkOffset{1});

  // build and execute table scan
  auto scan = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);

  // copy end execute copied table scan
  auto copied_scan = scan->deep_copy();
  EXPECT_NE(copied_scan, nullptr) << "Could not copy Scan";

  // table wrapper needs to be executed manually
  copied_scan->mutable_left_input()->execute();
  copied_scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_scan->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DiamondShape) {
  auto scan_a = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan_a->execute();

  auto scan_b = create_table_scan(scan_a, ColumnID{1}, PredicateCondition::LessThan, 1000);
  auto scan_c = create_table_scan(scan_a, ColumnID{1}, PredicateCondition::GreaterThan, 2000);
  auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);

  auto copied_pqp = union_positions->deep_copy();

  EXPECT_EQ(copied_pqp->left_input()->left_input(), copied_pqp->right_input()->left_input());
}

TEST_F(OperatorDeepCopyTest, Subquery) {
  // Due to the nested structure of the subquery, it makes sense to keep this more high level than the other tests in
  // this suite. The test would be very confusing and error-prone with explicit operators as above.
  const auto table = load_table("resources/test_data/tbl/int_int_int.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("table_3int", table);

  const std::string subquery_query = "SELECT * FROM table_3int WHERE a = (SELECT MAX(b) FROM table_3int)";
  const TableColumnDefinitions column_definitions = {
      {"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}};

  auto sql_pipeline = SQLPipelineBuilder{subquery_query}.disable_mvcc().create_pipeline();
  const auto [pipeline_status, first_result] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  // Quick sanity check to see that the original query is correct
  auto expected_first = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_first->append({10, 10, 10});
  EXPECT_TABLE_EQ_UNORDERED(first_result, expected_first);

  SQLPipelineBuilder{"INSERT INTO table_3int VALUES (11, 11, 11)"}.create_pipeline().get_result_table();

  const auto copied_plan = sql_pipeline.get_physical_plans()[0]->deep_copy();
  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(copied_plan);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  const auto copied_result = root_operator_task->get_operator()->get_output();

  auto expected_copied = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_copied->append({11, 10, 11});
  expected_copied->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(copied_result, expected_copied);
}

TEST_F(OperatorDeepCopyTest, DeduplicationAmongRootAndSubqueryPQPs) {
  /**
   * In this test, we check whether deep copies preserve deduplication for
   *  uncorrelated subqueries that share a part of the root PQP. Similar to TPC-H Q11.
   */
  auto get_table_a = std::make_shared<GetTable>(_table_name_a);
  auto get_table_b = std::make_shared<GetTable>(_table_name_b);

  // Prepare uncorrelated subquery that uses get_table_a from root PQP
  auto join = std::make_shared<JoinHash>(
      get_table_a, get_table_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  auto projection = std::make_shared<Projection>(join, expression_vector(_a_a));
  auto pqp_subquery_expression = std::make_shared<PQPSubqueryExpression>(projection);

  // Prepare Root PQP
  auto table_scan = std::make_shared<TableScan>(get_table_a, less_than_(_a_a, pqp_subquery_expression));

  // Check that the following condition survives deep_copy()
  ASSERT_EQ(get_table_a->consumer_count(), 2);

  auto copied_table_scan = table_scan->deep_copy();
  auto copied_get_table_a = copied_table_scan->left_input();
  EXPECT_EQ(copied_get_table_a->consumer_count(), get_table_a->consumer_count());
}

TEST_F(OperatorDeepCopyTest, DeduplicationAmongSubqueries) {
  /**
   * In this test, we check whether deep copies preserve deduplication for
   *  uncorrelated subqueries that share parts of their PQP among each other. Similar to TPC-DS Q9.
   */
  auto get_table_a = std::make_shared<GetTable>(_table_name_a);
  auto get_table_b = std::make_shared<GetTable>(_table_name_b);

  // Prepare three subqueries for Case expression
  auto group_by_columns = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  auto count_star = count_(pqp_column_(INVALID_COLUMN_ID, DataType::Long, false, "*"));
  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{count_star};
  auto aggregate_hash = std::make_shared<AggregateHash>(get_table_a, aggregates, group_by_columns);
  auto when_subquery = std::make_shared<PQPSubqueryExpression>(aggregate_hash);

  auto table_scan1 = std::make_shared<TableScan>(get_table_b, between_inclusive_(_b_b, 1, 20));
  auto then_subquery = std::make_shared<PQPSubqueryExpression>(table_scan1);

  auto table_scan2 = std::make_shared<TableScan>(get_table_b, between_inclusive_(_b_b, 21, 40));
  auto otherwise_subquery = std::make_shared<PQPSubqueryExpression>(table_scan2);

  // Root PQP
  auto projection = std::make_shared<Projection>(
      get_table_b, expression_vector(_a_a, case_(when_subquery, then_subquery, otherwise_subquery)));

  // Check that the following condition survives deep_copy()
  ASSERT_EQ(get_table_b->consumer_count(), 3);

  auto copied_projection = projection->deep_copy();
  auto copied_get_table_b = copied_projection->left_input();
  EXPECT_EQ(copied_get_table_b->consumer_count(), get_table_b->consumer_count());
}

}  // namespace hyrise
