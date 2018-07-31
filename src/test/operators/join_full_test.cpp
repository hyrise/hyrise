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
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Join implementations that
implement all operators, not just PredicateCondition::Equals.
*/

template <typename T>
class JoinFullTest : public JoinTest {};

// here we define all Join types
typedef ::testing::Types<JoinNestedLoop, JoinSortMerge, JoinIndex> JoinFullTypes;
TYPED_TEST_CASE(JoinFullTest, JoinFullTypes);

TYPED_TEST(JoinFullTest, CrossJoin) {
  if (!IS_DEBUG) return;

  EXPECT_THROW(std::make_shared<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b, JoinMode::Cross,
                                           ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals),
               std::logic_error);
}

TYPED_TEST(JoinFullTest, LeftJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, LeftJoinOnString) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             ColumnIDPair(ColumnID{1}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Left, "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, RightJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoinOnString) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             ColumnIDPair(ColumnID{1}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoinSingleChunk) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_e, this->_table_wrapper_f, ColumnIDPair(ColumnID{1}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join_single_chunk.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerValueDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictValueJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerValueDictRefJoin) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictValueRefJoin) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefDictJoinFiltered) {
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerJoinBig) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_c, this->_table_wrapper_d, ColumnIDPair(ColumnID{0}, ColumnID{1}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, InnerRefJoinFilteredBig) {
  auto scan_c =
      std::make_shared<TableScan>(this->_table_wrapper_c, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d =
      std::make_shared<TableScan>(this->_table_wrapper_d, ColumnID{1}, PredicateCondition::GreaterThanEquals, 6);
  scan_d->execute();

  this->template test_join_output<TypeParam>(scan_c, scan_d, ColumnIDPair(ColumnID{0}, ColumnID{1}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinFullTest, OuterJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, OuterJoinWithNull) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_m, this->_table_wrapper_n, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join_null.tbl", 1);
}

TYPED_TEST(JoinFullTest, OuterJoinDict) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::LessThan, JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{1}, ColumnID{1}),
      PredicateCondition::LessThan, JoinMode::Inner, "src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerInnerJoinDict) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::LessThan, JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{1}, ColumnID{1}),
      PredicateCondition::LessThan, JoinMode::Inner, "src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerInnerJoin2) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_j, this->_table_wrapper_i, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::LessThan, JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerOuterJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_k, this->_table_wrapper_l, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::LessThan, JoinMode::Outer, "src/test/tables/joinoperators/int_smaller_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_smallerequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{1}, ColumnID{1}), PredicateCondition::LessThanEquals,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/float_smallerequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerEqualInnerJoin2) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_j, this->_table_wrapper_i,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_smallerequal_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinFullTest, SmallerEqualOuterJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_k, this->_table_wrapper_l,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                                             JoinMode::Outer,
                                             "src/test/tables/joinoperators/int_smallerequal_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterInnerJoin) {
  // Joining two Integer Column
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::GreaterThan, JoinMode::Inner, "src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThan,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterInnerJoinDict) {
  // Joining two Integer Column
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::GreaterThan, JoinMode::Inner, "src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThan,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterInnerJoin2) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_i, this->_table_wrapper_j,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThan,
                                             JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_greater_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterOuterJoin) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_l, this->_table_wrapper_k, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::GreaterThan, JoinMode::Outer, "src/test/tables/joinoperators/int_greater_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{1}, ColumnID{1}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{1}, ColumnID{1}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterEqualOuterJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_l, this->_table_wrapper_k,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Outer,
                                             "src/test/tables/joinoperators/int_greaterequal_outer_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, GreaterEqualInnerJoin2) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(this->_table_wrapper_i, this->_table_wrapper_j,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::GreaterThanEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_greaterequal_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinFullTest, NotEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::NotEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{1}, ColumnID{1}),
      PredicateCondition::NotEquals, JoinMode::Inner, "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, NotEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::NotEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{1}, ColumnID{1}),
      PredicateCondition::NotEquals, JoinMode::Inner, "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnMixedValueAndDictionaryColumns) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c_dict, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnReferenceColumnAndValue) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnValueAndReferenceColumn) {
  // scan that returns all rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThan, 100);
  scan_b->execute();

  this->template test_join_output<TypeParam>(this->_table_wrapper_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::NotEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_neq.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinLessThanOnDictAndDict) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::LessThanEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_float_leq_dict.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnReferenceColumnAndDict) {
  // scan that returns all rows
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinFullTest, JoinOnDictAndReferenceColumn) {
  // scan that returns all rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThan, 100);
  scan_b->execute();

  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::NotEquals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join_neq.tbl", 1);
}

}  // namespace opossum
