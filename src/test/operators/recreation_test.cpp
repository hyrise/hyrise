#include <memory>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_hash.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/join_nested_loop_b.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "types.hpp"

namespace opossum {

class RecreationTest : public BaseTest {
  protected:
  void SetUp() override {
    // load and create regular ValueColumn tables
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float2.tbl", 2));

    // execute all TableWrapper operators in advance
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b;
};

template <typename T>
class RecreationTestJoin : public RecreationTest {};

// here we define all Join types
using JoinTypes = ::testing::Types<JoinNestedLoopA, JoinNestedLoopB, JoinHash, JoinSortMerge>;
TYPED_TEST_CASE(RecreationTestJoin, JoinTypes);


TYPED_TEST(RecreationTestJoin, RecreationJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_left_join.tbl", 1);
  EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

  // build and execute join
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b, JoinMode::Left, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals);
  EXPECT_NE(join, nullptr) << "Could not build Join";
  join->execute();
  this->EXPECT_TABLE_EQ(join->get_output(), expected_result);

  // recreate and execute recreated join
  auto recreated_join = join->recreate();
  EXPECT_NE(recreated_join, nullptr) << "Could not recreate Join";
  // table wrappers need to be executed manually
  recreated_join->mutable_input_left()->execute();
  recreated_join->mutable_input_right()->execute();
  recreated_join->execute();
  this->EXPECT_TABLE_EQ(recreated_join->get_output(), expected_result);
}

TEST_F(RecreationTest, RecreationTableScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  // build and execute table scan
  auto scan = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan->execute();
  EXPECT_TABLE_EQ(scan->get_output(), expected_result);

  // recreate and execute recreated table scan
  auto recreated_scan = scan->recreate();
  EXPECT_NE(recreated_scan, nullptr) << "Could not recreate Scan";
  // table wrappers need to be executed manually
  recreated_scan->mutable_input_left()->execute();
  recreated_scan->execute();
  EXPECT_TABLE_EQ(recreated_scan->get_output(), expected_result);
}

}  // namespace opossum
