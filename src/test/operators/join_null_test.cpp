#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for null value support for join implementations.
*/

template <typename T>
class JoinNullTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_a_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));

    _table_wrapper_a_null->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a_null;
};

using JoinNullTypes = ::testing::Types<JoinHash, JoinSortMerge>;
TYPED_TEST_CASE(JoinNullTest, JoinNullTypes);

TYPED_TEST(JoinNullTest, InnerJoinWithNull) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_a_null, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_float_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, LeftJoinWithNull) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_null, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, RightJoinWithNull) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_null, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join_null.tbl", 1);
}

}  // namespace opossum
