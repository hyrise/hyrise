#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Join implementations that only implement ScanType::Equals.
*/

template <typename T>
class JoinEquiTest : public JoinTest {};

// here we define all Join types
using JoinEquiTypes = ::testing::Types<JoinNestedLoop, JoinHash, JoinSortMerge>;
TYPED_TEST_CASE(JoinEquiTest, JoinEquiTypes);

TYPED_TEST(JoinEquiTest, WrongJoinOperator) {
  if (!IS_DEBUG) return;
  EXPECT_THROW(
      std::make_shared<JoinHash>(this->_table_wrapper_a, this->_table_wrapper_b, JoinMode::Left,
                                 std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::GreaterThan),
      std::logic_error);
}

TYPED_TEST(JoinEquiTest, LeftJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinOnString) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}),
      ScanType::Equals, JoinMode::Left, "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, OuterJoin) {
  if (std::is_same<TypeParam, JoinHash>::value) {
    return;
  }
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinOnString) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b_dict, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinBig) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFilteredBig) {
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_c, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = std::make_shared<TableScan>(this->_table_wrapper_d, ColumnID{1}, ScanType::GreaterThanEquals, 6);
  scan_d->execute();

  this->template test_join_output<TypeParam>(scan_c, scan_d, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}),
                                             ScanType::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, SelfJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_a, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Self, "src/test/tables/joinoperators/int_self_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndDictionaryColumns) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_c_dict, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndReferenceColumns) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeft) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(scan_a, scan_b, JoinMode::Inner,
                                          std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals, JoinMode::Inner,
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceRight) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(scan_a, scan_b, JoinMode::Inner,
                                          std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      scan_c, join, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals, JoinMode::Inner,
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_right.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeftFiltered) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, ColumnID{0}, ScanType::GreaterThan, 6);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(scan_a, scan_b, JoinMode::Inner,
                                          std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals, JoinMode::Inner,
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnValue) {
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Inner,
                                          std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_multijoin_val_val_val_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnRefOuter) {
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                          std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_multijoin_val_val_val_leftouter.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixNestedLoopAndHash) {
  auto join =
      std::make_shared<JoinNestedLoop>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                       std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixHashAndNestedLoop) {
  auto join = std::make_shared<JoinHash>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoinRefColumn) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinRefColumn) {
  // scan that returns all rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoinEmptyRefColumn) {
  // scan that returns no rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::Equals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Right, "src/test/tables/joinoperators/int_join_empty.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinEmptyRefColumn) {
  // scan that returns no rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::Equals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      this->_table_wrapper_b, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Left, "src/test/tables/joinoperators/int_join_empty_left.tbl", 1);
}

// Does not work yet due to problems with RowID implementation (RowIDs need to reference a table)
TYPED_TEST(JoinEquiTest, DISABLED_JoinOnUnion /* #160 */) {
  //  Filtering to generate RefColumns
  auto filtered_left =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_e, ColumnID{0}, ScanType::LessThanEquals, 10);
  filtered_left->execute();
  auto filtered_left2 =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_f, ColumnID{0}, ScanType::LessThanEquals, 10);
  filtered_left2->execute();
  auto filtered_right =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_g, ColumnID{0}, ScanType::LessThanEquals, 10);
  filtered_right->execute();
  auto filtered_right2 =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_h, ColumnID{0}, ScanType::LessThanEquals, 10);
  filtered_right2->execute();

  // Union left and right
  auto union_left = std::make_shared<opossum::UnionAll>(filtered_left, filtered_left2);
  union_left->execute();
  auto union_right = std::make_shared<opossum::UnionAll>(filtered_right, filtered_right2);
  union_right->execute();

  this->template test_join_output<TypeParam>(
      union_left, union_right, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals,
      JoinMode::Inner, "src/test/tables/joinoperators/expected_join_result_1.tbl", 1);
}

}  // namespace opossum
