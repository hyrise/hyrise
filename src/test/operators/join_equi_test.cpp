#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Join implementations that only implement PredicateCondition::Equals.
*/

template <typename T>
class JoinEquiTest : public JoinTest {};

// here we define all Join types
using JoinEquiTypes = ::testing::Types<JoinNestedLoop, JoinHash, JoinSortMerge, JoinIndex, JoinMPSM>;
TYPED_TEST_CASE(JoinEquiTest, JoinEquiTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(JoinEquiTest, LeftJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                                             "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinIntFloat) {
  if constexpr (std::is_same_v<TypeParam, JoinSortMerge> || std::is_same_v<TypeParam, JoinMPSM>) {
    return;
  }

  // int with float
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_o,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_float_inner.tbl", 1);

  // float with int
  this->template test_join_output<TypeParam>(this->_table_wrapper_o, this->_table_wrapper_a,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/float_int_inner.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinIntFloatRadixBit) {
  if constexpr (std::is_same_v<TypeParam, JoinHash>) {
    // float with int
    // radix bits = 0
    std::shared_ptr<Table> expected_result =
        load_table("resources/test_data/tbl/join_operators/float_int_inner.tbl", 1);
    auto join =
        std::make_shared<JoinHash>(this->_table_wrapper_o, this->_table_wrapper_a, JoinMode::Inner,
                                   OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, 0);
    join->execute();
    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);

    // radix_bits==8 creates 2^8 clusters to check for the case when #clusters > #rows.
    for (size_t radix_bits : {1, 2, 8}) {
      auto join_comp = std::make_shared<JoinHash>(
          this->_table_wrapper_o, this->_table_wrapper_a, JoinMode::Inner,
          OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, radix_bits);
      join_comp->execute();
      EXPECT_TABLE_EQ_UNORDERED(join->get_output(), join_comp->get_output());
    }
  }
}

TYPED_TEST(JoinEquiTest, InnerJoinIntDouble) {
  if constexpr (std::is_same_v<TypeParam, JoinSortMerge> || std::is_same_v<TypeParam, JoinMPSM>) {
    return;
  }

  // int with double
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_p,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_double_inner.tbl", 1);

  // double with int
  this->template test_join_output<TypeParam>(this->_table_wrapper_p, this->_table_wrapper_a,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/double_int_inner.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                                             "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1);
}

TYPED_TEST(JoinEquiTest, OuterJoin) {
  if constexpr (std::is_same_v<TypeParam, JoinHash>) {
    return;
  }
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::FullOuter, "resources/test_data/tbl/join_operators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinOnString) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a, this->_table_wrapper_b_dict,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerValueDictRefJoin) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      this->create_table_scan(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictValueRefJoin) {
  // scan that returns all rows
  auto scan_a =
      this->create_table_scan(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFiltered) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerDictJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_dict, this->_table_wrapper_b_dict,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a =
      this->create_table_scan(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      this->create_table_scan(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefDictJoinFiltered) {
  auto scan_a =
      this->create_table_scan(this->_table_wrapper_a_dict, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b =
      this->create_table_scan(this->_table_wrapper_b_dict, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerJoinBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c, this->_table_wrapper_d,
                                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, InnerRefJoinFilteredBig) {
  auto scan_c = this->create_table_scan(this->_table_wrapper_c, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = this->create_table_scan(this->_table_wrapper_d, ColumnID{1}, PredicateCondition::GreaterThanEquals, 6);
  scan_d->execute();

  this->template test_join_output<TypeParam>(
      scan_c, scan_d, {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndDictionarySegments) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_c_dict, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, JoinOnMixedValueAndReferenceSegments) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeft) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceRight) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      scan_c, join, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_right.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnReferenceLeftFiltered) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThan, 6);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<TypeParam>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_left_filtered.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnValue) {
  auto join =
      std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Inner,
                                  OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_val_val_val_left.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MultiJoinOnRefOuter) {
  auto join =
      std::make_shared<TypeParam>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                  OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_val_val_val_leftouter.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixNestedLoopAndHash) {
  auto join =
      std::make_shared<JoinNestedLoop>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                       OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, MixHashAndNestedLoop) {
  auto join = std::make_shared<JoinHash>(
      this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  this->template test_join_output<TypeParam>(
      join, this->_table_wrapper_h, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
      "resources/test_data/tbl/join_operators/int_inner_multijoin_nlj_hash.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoinRefSegment) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                                             "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinRefSegment) {
  // scan that returns all rows
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(this->_table_wrapper_a, scan_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                                             "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1);
}

TYPED_TEST(JoinEquiTest, RightJoinEmptyRefSegment) {
  // scan that returns no rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_a->execute();

  this->template test_join_output<TypeParam>(scan_a, this->_table_wrapper_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                                             "resources/test_data/tbl/join_operators/int_join_empty.tbl", 1);
}

TYPED_TEST(JoinEquiTest, LeftJoinEmptyRefSegment) {
  // scan that returns no rows
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(this->_table_wrapper_b, scan_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                                             "resources/test_data/tbl/join_operators/int_join_empty_left.tbl", 1);
}

// Does not work yet due to problems with RowID implementation (RowIDs need to reference a table)
TYPED_TEST(JoinEquiTest, DISABLED_JoinOnUnion /* #160 */) {
  //  Filtering to generate RefSegments
  auto filtered_left =
      this->create_table_scan(this->_table_wrapper_e, ColumnID{0}, PredicateCondition::LessThanEquals, 10);
  filtered_left->execute();
  auto filtered_left2 =
      this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::LessThanEquals, 10);
  filtered_left2->execute();
  auto filtered_right =
      this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::LessThanEquals, 10);
  filtered_right->execute();
  auto filtered_right2 =
      this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::LessThanEquals, 10);
  filtered_right2->execute();

  // Union left and right
  auto union_left = std::make_shared<opossum::UnionAll>(filtered_left, filtered_left2);
  union_left->execute();
  auto union_right = std::make_shared<opossum::UnionAll>(filtered_right, filtered_right2);
  union_right->execute();

  this->template test_join_output<TypeParam>(union_left, union_right,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                                             "resources/test_data/tbl/join_operators/expected_join_result_1.tbl", 1);
}

}  // namespace opossum
