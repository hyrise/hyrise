#include <memory>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/difference.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

// At the moment all the recreation methods just call the constructor again. At first sight, these tests
// do not seem to add too much value because. This might change in the future. Then, these tests will
// make much more sense.

class RecreationTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float2.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float3.tbl", 2));
    _table_wrapper_d = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int3.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();

    _test_get_table = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data, 2);
    StorageManager::get().add_table("aNiceTestTable", _test_get_table);
  }

  std::shared_ptr<Table> _test_get_table;
  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d;
};

template <typename T>
class RecreationTestJoin : public RecreationTest {};

// here we define all Join types
using JoinTypes = ::testing::Types<JoinNestedLoop, JoinHash, JoinSortMerge>;
TYPED_TEST_CASE(RecreationTestJoin, JoinTypes);

TYPED_TEST(RecreationTestJoin, RecreationJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_left_join.tbl", 1);
  EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

  // build and execute join
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b, JoinMode::Left,
                                          ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  EXPECT_NE(join, nullptr) << "Could not build Join";
  join->execute();
  EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);

  // recreate and execute recreated join
  auto recreated_join = join->recreate();
  EXPECT_NE(recreated_join, nullptr) << "Could not recreate Join";

  // table wrappers need to be executed manually
  recreated_join->mutable_input_left()->execute();
  recreated_join->mutable_input_right()->execute();
  recreated_join->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_join->get_output(), expected_result);
}

TEST_F(RecreationTest, RecreationDifference) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  // build and execute difference
  auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_c);
  difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(difference->get_output(), expected_result);

  // recreate and execute recreated difference
  auto recreated_difference = difference->recreate();
  EXPECT_NE(recreated_difference, nullptr) << "Could not recreate Difference";

  // table wrapper needs to be executed manually
  recreated_difference->mutable_input_left()->execute();
  recreated_difference->mutable_input_right()->execute();
  recreated_difference->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_difference->get_output(), expected_result);
}

TEST_F(RecreationTest, RecreationGetTable) {
  // build and execute get table
  auto get_table = std::make_shared<GetTable>("aNiceTestTable");
  get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(get_table->get_output(), _test_get_table);

  // recreate and execute recreated get table
  auto recreated_get_table = get_table->recreate();
  EXPECT_NE(recreated_get_table, nullptr) << "Could not recreate GetTable";

  recreated_get_table->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_get_table->get_output(), _test_get_table);
}

TEST_F(RecreationTest, RecreationLimit) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int3_limit_1.tbl", 1);

  // build and execute limit
  auto limit = std::make_shared<Limit>(_table_wrapper_d, 1);
  limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(limit->get_output(), expected_result);

  // recreate and execute recreated limit
  auto recreated_limit = limit->recreate();
  EXPECT_NE(recreated_limit, nullptr) << "Could not recreate Limit";

  // table wrapper needs to be executed manually
  recreated_limit->mutable_input_left()->execute();
  recreated_limit->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_limit->get_output(), expected_result);
}

TEST_F(RecreationTest, RecreationSort) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 1);

  // build and execute sort
  auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(sort->get_output(), expected_result);

  // recreate and execute recreated sort
  auto recreated_sort = sort->recreate();
  EXPECT_NE(recreated_sort, nullptr) << "Could not recreate Sort";

  // table wrapper needs to be executed manually
  recreated_sort->mutable_input_left()->execute();
  recreated_sort->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_sort->get_output(), expected_result);
}

TEST_F(RecreationTest, RecreationTableScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  // build and execute table scan
  auto scan =
      std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);

  // recreate and execute recreated table scan
  auto recreated_scan = scan->recreate();
  EXPECT_NE(recreated_scan, nullptr) << "Could not recreate Scan";

  // table wrapper needs to be executed manually
  recreated_scan->mutable_input_left()->execute();
  recreated_scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(recreated_scan->get_output(), expected_result);
}

TEST_F(RecreationTest, DiamondShape) {
  auto scan_a = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  auto scan_b = std::make_shared<TableScan>(scan_a, ColumnID{1}, PredicateCondition::LessThan, 1000);
  auto scan_c = std::make_shared<TableScan>(scan_a, ColumnID{1}, PredicateCondition::GreaterThan, 2000);
  auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);

  auto recreated_pqp = union_positions->recreate();

  EXPECT_EQ(recreated_pqp->input_left()->input_left(), recreated_pqp->input_right()->input_left());
}

TEST_F(RecreationTest, Subselect) {
  // Due to the nested structure of the subselect, it makes sense to keep this more high level than the other tests in
  // this suite. The test is very confusing and error-prone with explicit operators as above.
  const auto table = load_table("src/test/tables/int_int_int.tbl", 2);
  StorageManager::get().add_table("table_3int", table);

  const std::string subselect_query = "SELECT * FROM table_3int WHERE a = (SELECT MAX(b) FROM table_3int)";
  const TableColumnDefinitions column_definitions = {{"a", DataType::Int}, {"b", DataType::Int}, {"c", DataType::Int}};

  SQLPipelineStatement sql_pipeline{subselect_query, UseMvcc::No};
  const auto first_result = sql_pipeline.get_result_table();

  // Quick sanity check to see that the original query is correct
  auto expected_first = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_first->append({10, 10, 10});
  EXPECT_TABLE_EQ_UNORDERED(first_result, expected_first);

  SQLPipelineStatement{"INSERT INTO table_3int VALUES (11, 11, 11)"}.get_result_table();

  const auto recreated_plan = sql_pipeline.get_query_plan()->recreate();
  const auto tasks = recreated_plan.create_tasks();
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  const auto recreated_result = tasks.back()->get_operator()->get_output();

  auto expected_recreated = std::make_shared<Table>(column_definitions, TableType::Data);
  expected_recreated->append({11, 10, 11});
  expected_recreated->append({11, 11, 11});

  EXPECT_TABLE_EQ_UNORDERED(recreated_result, expected_recreated);
}

}  // namespace opossum
