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
#include "../../lib/operators/join_nested_loop_a.hpp"
#include "../../lib/operators/join_nested_loop_b.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

/*
This contains the tests for Join implementations that
implement all operators, not just "=".
*/

template <typename T>
class JoinFullTest : public JoinTest {};

// here we define all Join types
typedef ::testing::Types<JoinNestedLoopA, JoinNestedLoopB /* , SortMergeJoin */> JoinFullTypes;
TYPED_TEST_CASE(JoinFullTest, JoinFullTypes);

TYPED_TEST(JoinFullTest, CrossJoin) {
  // this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
  //                                            "=", Cross, "src/test/tables/joinoperators/int_cross_join.tbl", 1);

  EXPECT_THROW(std::make_shared<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"), "=",
                                           Cross, std::string("left."), std::string("right.")),
               std::runtime_error);
}

TYPED_TEST(JoinFullTest, LeftJoin) {
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "=", Left, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, LeftJoinOnString) {
  this->template test_join_output<TypeParam>(this->_gt_c, this->_gt_d, std::pair<std::string, std::string>("b", "b"),
                                             "=", Left, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, RightJoin) {
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "=", Right, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoin) {
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoinOnString) {
  this->template test_join_output<TypeParam>(this->_gt_c, this->_gt_d, std::pair<std::string, std::string>("b", "b"),
                                             "=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoin) {
  this->_gt_a->execute();
  this->_gt_b->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_gt_a, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerValueDictJoin) {
  this->_gt_a->execute();
  this->_gt_b_dict->execute();

  this->template test_join_output<TypeParam>(
      this->_gt_a, this->_gt_b_dict, std::pair<std::string, std::string>("a", "a"), "=", Inner, std::string("left."),
      std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictValueJoin) {
  this->_gt_a_dict->execute();
  this->_gt_b->execute();

  this->template test_join_output<TypeParam>(
      this->_gt_a_dict, this->_gt_b, std::pair<std::string, std::string>("a", "a"), "=", Inner, std::string("left."),
      std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerValueDictRefJoin) {
  this->_gt_a->execute();
  this->_gt_b_dict->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_gt_a, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictValueRefJoin) {
  this->_gt_a_dict->execute();
  this->_gt_b->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_gt_a_dict, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoinFiltered) {
  this->_gt_a->execute();
  this->_gt_b->execute();

  auto scan_a = std::make_shared<TableScan>(this->_gt_a, "a", ">", 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictJoin) {
  this->_gt_a_dict->execute();
  this->_gt_b_dict->execute();

  this->template test_join_output<TypeParam>(
      this->_gt_a_dict, this->_gt_b_dict, std::pair<std::string, std::string>("a", "a"), "=", Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefDictJoin) {
  this->_gt_a_dict->execute();
  this->_gt_b_dict->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_gt_a_dict, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefDictJoinFiltered) {
  this->_gt_a_dict->execute();
  this->_gt_b_dict->execute();

  auto scan_a = std::make_shared<TableScan>(this->_gt_a_dict, "a", ">", 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoinBig) {
  this->_gt_c->execute();
  this->_gt_d->execute();

  this->template test_join_output<TypeParam>(this->_gt_c, this->_gt_d, std::pair<std::string, std::string>("a", "a"),
                                             "=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoinFilteredBig) {
  this->_gt_c->execute();
  this->_gt_d->execute();

  auto scan_c = std::make_shared<TableScan>(this->_gt_c, "a", ">=", 0);
  scan_c->execute();
  auto scan_d = std::make_shared<TableScan>(this->_gt_d, "a", ">=", 6);
  scan_d->execute();

  this->template test_join_output<TypeParam>(scan_c, scan_d, std::pair<std::string, std::string>("a", "a"), "=", Inner,
                                             std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, DISABLED_OuterJoin) {
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "=", Outer, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

// This is not implemented yet.
TYPED_TEST(JoinFullTest, DISABLED_NaturalJoin) {
  this->_gt_a->execute();
  this->_gt_b->execute();

  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "=", Natural, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_natural_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SelfJoin) {
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_a, std::pair<std::string, std::string>("a", "a"),
                                             "=", Self, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_self_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "<", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("b", "b"),
                                             "<", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "<=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_smallerequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("b", "b"),
                                             "<=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/float_smallerequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterInnerJoin) {
  // Joining two Integer Column
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             ">", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("b", "b"),
                                             ">", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             ">=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("b", "b"),
                                             ">=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, NotEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"),
                                             "!=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_gt_a, this->_gt_b, std::pair<std::string, std::string>("b", "b"),
                                             "!=", Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnMixedValueAndDictionaryColumns) {
  this->template test_join_output<TypeParam>(
      this->_gt_c_dict, this->_gt_b, std::pair<std::string, std::string>("a", "a"), "=", Inner, std::string("left."),
      std::string("right."), "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnMixedValueAndReferenceColumns) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_gt_a, "a", ">=", 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_gt_b, std::pair<std::string, std::string>("a", "a"), "=",
                                             Inner, std::string("left."), std::string("right."),
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

}  // namespace opossum
