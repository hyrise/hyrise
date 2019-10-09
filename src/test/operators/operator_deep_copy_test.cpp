#include <memory>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/difference.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

// At the moment all the deep_copy() methods just call the constructor again. At first sight, these tests
// do not seem to add too much value because. This might change in the future. Then, these tests will
// make much more sense.

class OperatorDeepCopyTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float3.tbl", 2));
    _table_wrapper_d = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int3.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();

    _test_table = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("aNiceTestTable", _test_table);
  }

  std::shared_ptr<Table> _test_table;
  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d;
};

template <typename T>
class DeepCopyTestJoin : public OperatorDeepCopyTest {};

// here we define all Join types
using JoinTypes = ::testing::Types<JoinNestedLoop, JoinHash, JoinSortMerge, JoinMPSM>;
TYPED_TEST_SUITE(DeepCopyTestJoin, JoinTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(DeepCopyTestJoin, DeepCopyJoin) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1);
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
  copied_join->mutable_input_left()->execute();
  copied_join->mutable_input_right()->execute();
  copied_join->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_join->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyDifference) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 2);

  // build and execute difference
  auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_c);
  difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(difference->get_output(), expected_result);

  // Copy and execute copies difference
  auto copied_difference = difference->deep_copy();
  EXPECT_NE(copied_difference, nullptr) << "Could not copy Difference";

  // table wrapper needs to be executed manually
  copied_difference->mutable_input_left()->execute();
  copied_difference->mutable_input_right()->execute();
  copied_difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_difference->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyGetTable) {
  // build and execute get table
  auto get_table = std::make_shared<GetTable>("aNiceTestTable");
  get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(get_table->get_output(), _test_table);

  // copy end execute copied get table
  auto copied_get_table = get_table->deep_copy();
  EXPECT_NE(copied_get_table, nullptr) << "Could not copy GetTable";

  copied_get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_get_table->get_output(), _test_table);
}

TEST_F(OperatorDeepCopyTest, DeepCopyLimit) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_int3_limit_1.tbl", 1);

  // build and execute limit
  auto limit = std::make_shared<Limit>(_table_wrapper_d, to_expression(int64_t{1}));
  limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(limit->get_output(), expected_result);

  // copy end execute copied limit
  auto copied_limit = limit->deep_copy();
  EXPECT_NE(copied_limit, nullptr) << "Could not copy Limit";

  // table wrapper needs to be executed manually
  copied_limit->mutable_input_left()->execute();
  copied_limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_limit->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopySort) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 1);

  // build and execute sort
  auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(sort->get_output(), expected_result);

  // copy end execute copied sort
  auto copied_sort = sort->deep_copy();
  EXPECT_NE(copied_sort, nullptr) << "Could not copy Sort";

  // table wrapper needs to be executed manually
  copied_sort->mutable_input_left()->execute();
  copied_sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(copied_sort->get_output(), expected_result);
}

TEST_F(OperatorDeepCopyTest, DeepCopyTableScan) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 1);

  // build and execute table scan
  auto scan = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);

  // copy end execute copied table scan
  auto copied_scan = scan->deep_copy();
  EXPECT_NE(copied_scan, nullptr) << "Could not copy Scan";

  // table wrapper needs to be executed manually
  copied_scan->mutable_input_left()->execute();
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

  EXPECT_EQ(copied_pqp->input_left()->input_left(), copied_pqp->input_right()->input_left());
}

TEST_F(OperatorDeepCopyTest, Subquery) {
  // Due to the nested structure of the subquery, it makes sense to keep this more high level than the other tests in
  // this suite. The test is very confusing and error-prone with explicit operators as above.
  const auto table = load_table("resources/test_data/tbl/int_int_int.tbl", 2);
  Hyrise::get().storage_manager.add_table("table_3int", table);

  const std::string subquery_query = "SELECT * FROM table_3int WHERE a = (SELECT MAX(b) FROM table_3int)";
  const TableColumnDefinitions column_definitions = {
      {"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}};

  auto sql_pipeline = SQLPipelineBuilder{subquery_query}.disable_mvcc().create_pipeline_statement();
  const auto [pipeline_status, first_result] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  // Quick sanity check to see that the original query is correct
  auto expected_first = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_first->append({10, 10, 10});
  EXPECT_TABLE_EQ_UNORDERED(first_result, expected_first);

  SQLPipelineBuilder{"INSERT INTO table_3int VALUES (11, 11, 11)"}.create_pipeline_statement().get_result_table();

  const auto copied_plan = sql_pipeline.get_physical_plan()->deep_copy();
  const auto tasks = OperatorTask::make_tasks_from_operator(copied_plan, CleanupTemporaries::Yes);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  const auto copied_result = tasks.back()->get_operator()->get_output();

  auto expected_copied = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_copied->append({11, 10, 11});
  expected_copied->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(copied_result, expected_copied);
}

}  // namespace opossum
