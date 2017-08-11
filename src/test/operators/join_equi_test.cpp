#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/join_hash.hpp"
#include "../../lib/operators/sort_merge_join.hpp"
#include "../../lib/operators/join_nested_loop_a.hpp"
#include "../../lib/operators/join_nested_loop_b.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/union_all.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

/*
This contains the tests for Join implementations that only implement ScanType::OpEquals.
*/

template <typename T>
class JoinEquiTest : public JoinTest {};

// here we define all Join types
using JoinEquiTypes = ::testing::Types<JoinNestedLoopA, JoinNestedLoopB, JoinHash, SortMergeJoin>;
TYPED_TEST_CASE(JoinEquiTest, JoinEquiTypes);

TYPED_TEST(JoinEquiTest, WrongJoinOperator) {
  if (!IS_DEBUG) return;
  EXPECT_THROW(std::make_shared<JoinHash>(this->_table_wrapper_a, this->_table_wrapper_b,
                                          std::pair<std::string, std::string>("a", "a"), ScanType::OpGreaterThan,
                                          JoinMode::Left, std::string("left."), std::string("right.")),
               std::logic_error);
}

TYPED_TEST(JoinEquiTest, LeftJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Left, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinOnString) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             std::pair<std::string, std::string>("b", "b"), ScanType::OpEquals,
                                             JoinMode::Left, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Right, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

// Currently not implemented for Hash Join, thus disabled
TYPED_TEST(JoinEquiTest, DISABLED_OuterJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Outer, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinOnString) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             std::pair<std::string, std::string>("b", "b"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b_dict,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, "a", ScanType::OpGreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, "a", ScanType::OpGreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFilteredBig) {
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_c, "a", ScanType::OpGreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = std::make_shared<TableScan>(this->_table_wrapper_d, "a", ScanType::OpGreaterThanEquals, 6);
  scan_d->execute();

  this->template test_join_output<TypeParam>(scan_c, scan_d, std::pair<std::string, std::string>("a", "a"),
                                             ScanType::OpEquals, JoinMode::Inner, std::string("left."),
                                             std::string("right."),
                                             "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, SelfJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_a,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Self, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_self_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndDictionaryColumns) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c_dict, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndReferenceColumns) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b,
                                             std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeft) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, "a", ScanType::OpGreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                  JoinMode::Inner, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."),
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceRight) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, "a", ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, "a", ScanType::OpGreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                  JoinMode::Inner, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(
      scan_c, join, std::pair<std::string, std::string>("a", "left.a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."),
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_right.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeftFiltered) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_f, "a", ScanType::OpGreaterThan, 6);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_g, "a", ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_h, "a", ScanType::OpGreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                  JoinMode::Inner, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."),
      "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnValue) {
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g,
                                          std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                          JoinMode::Inner, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals,
      JoinMode::Inner, std::string("left."), std::string("right."),
      "src/test/tables/joinoperators/int_inner_multijoin_val_val_val_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnRefOuter) {
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g,
                                          std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                          JoinMode::Left, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals,
      JoinMode::Inner, std::string("left."), std::string("right."),
      "src/test/tables/joinoperators/int_inner_multijoin_val_val_val_leftouter.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixNestedLoopAAndHash) {
  auto join = std::make_shared<JoinNestedLoopA>(this->_table_wrapper_f, this->_table_wrapper_g,
                                                std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                                JoinMode::Left, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(join, this->_table_wrapper_h,
                                             std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixNestedLoopBAndHash) {
  auto join = std::make_shared<JoinNestedLoopB>(this->_table_wrapper_f, this->_table_wrapper_g,
                                                std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                                JoinMode::Left, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(join, this->_table_wrapper_h,
                                             std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixHashAndNestedLoop) {
  auto join = std::make_shared<JoinHash>(this->_table_wrapper_f, this->_table_wrapper_g,
                                         std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                         JoinMode::Left, std::string("left."), std::string("right."));
  join->execute();

  this->template test_join_output<TypeParam>(join, this->_table_wrapper_h,
                                             std::pair<std::string, std::string>("left.a", "a"), ScanType::OpEquals,
                                             JoinMode::Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, AppliesPrefixes) {
  auto join = std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g,
                                          std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals,
                                          JoinMode::Left, std::string("pref1."), std::string("pref2."));
  join->execute();

  ASSERT_EQ(join->get_output()->column_name(ColumnID{0}), "pref1.a");
  ASSERT_EQ(join->get_output()->column_name(ColumnID{1}), "pref1.b");
  ASSERT_EQ(join->get_output()->column_name(ColumnID{2}), "pref2.a");
  ASSERT_EQ(join->get_output()->column_name(ColumnID{3}), "pref2.b");
}

TYPED_TEST(JoinEquiTest, ColumnsNotOptional) {
  if (!IS_DEBUG) return;
  EXPECT_THROW(std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g, nullopt, ScanType::OpEquals,
                                           JoinMode::Left, std::string("left."), std::string("right.")),
               std::logic_error);
}

// Does not work yet due to problems with RowID implementation (RowIDs need to reference a table)
TYPED_TEST(JoinEquiTest, DISABLED_JoinOnRefColumns) {
  //  Filtering to generate RefColumns
  auto filtered_left =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_e, "a", ScanType::OpLessThanEquals, 10);
  filtered_left->execute();
  auto filtered_left2 =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_f, "a", ScanType::OpLessThanEquals, 10);
  filtered_left2->execute();
  auto filtered_right =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_g, "a", ScanType::OpLessThanEquals, 10);
  filtered_right->execute();
  auto filtered_right2 =
      std::make_shared<opossum::TableScan>(this->_table_wrapper_h, "a", ScanType::OpLessThanEquals, 10);
  filtered_right2->execute();

  // Union left and right
  auto union_left = std::make_shared<opossum::UnionAll>(filtered_left, filtered_left2);
  union_left->execute();
  auto union_right = std::make_shared<opossum::UnionAll>(filtered_right, filtered_right2);
  union_right->execute();

  this->template test_join_output<TypeParam>(
      union_left, union_right, std::pair<std::string, std::string>("a", "a"), ScanType::OpEquals, JoinMode::Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/expected_join_result_1.tbl", 1);
}

}  // namespace opossum
