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

#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Semi-, AntiNullAsTrue and AntiRetainsNull-Join implementations.
*/
template <typename TypeParam>
class JoinSemiAntiTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_int_int_string_nulls_random = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/int_int_string_nulls_random.tbl", 2));
    _table_wrapper_semi_a =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/join_operators/semi_left.tbl", 2));
    _table_wrapper_semi_b =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/join_operators/semi_right.tbl", 2));

    _table_wrapper_int_int_string_nulls_random->execute();
    _table_wrapper_semi_a->execute();
    _table_wrapper_semi_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_int_int_string_nulls_random, _table_wrapper_semi_a,
      _table_wrapper_semi_b;
};

using JoinSemiAntiTypes = ::testing::Types<JoinHash, JoinNestedLoop, JoinSortMerge>;
TYPED_TEST_CASE(JoinSemiAntiTest, JoinSemiAntiTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(JoinSemiAntiTest, SemiJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_k, this->_table_wrapper_a,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/int.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, SemiJoinRefSegments) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::Semi, "resources/test_data/tbl/int.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, SemiJoinBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_semi_a, this->_table_wrapper_semi_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/join_operators/semi_result.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, AntiJoinWithoutNulls) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_k, this->_table_wrapper_a, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/anti_int4.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_k, this->_table_wrapper_a, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsFalse, "resources/test_data/tbl/join_operators/anti_int4.tbl");
}

TYPED_TEST(JoinSemiAntiTest, AntiJoinWithoutNullsJoinRefSegments) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::AntiNullAsTrue,
                                             "resources/test_data/tbl/join_operators/anti_int4.tbl", 1);
  this->template test_join_output<TypeParam>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::AntiNullAsFalse,
                                             "resources/test_data/tbl/join_operators/anti_int4.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, AntiWithoutNullsBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_semi_a, this->_table_wrapper_semi_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::AntiNullAsTrue,
                                             "resources/test_data/tbl/join_operators/anti_result.tbl", 1);
  this->template test_join_output<TypeParam>(this->_table_wrapper_semi_a, this->_table_wrapper_semi_b,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                             JoinMode::AntiNullAsFalse,
                                             "resources/test_data/tbl/join_operators/anti_result.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, SemiWithNulls) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_r, this->_table_wrapper_r,
                                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(this->_table_wrapper_r, this->_table_wrapper_r,
                                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(this->_table_wrapper_r, this->_table_wrapper_r,
                                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(this->_table_wrapper_r, this->_table_wrapper_r,
                                             {{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Semi,
                                             "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
}

TYPED_TEST(JoinSemiAntiTest, AntiNullAsFalseWithNullsBasic) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsFalse, "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsFalse, "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsFalse, "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsFalse, "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
}

TYPED_TEST(JoinSemiAntiTest, AntiNullAsFalseWithNullsLarger) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
      "resources/test_data/tbl/join_operators/anti_null_as_false_result_a.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
      "resources/test_data/tbl/join_operators/anti_null_as_false_result_b.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{2}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
      "resources/test_data/tbl/join_operators/anti_null_as_false_result_c.tbl");
}

TYPED_TEST(JoinSemiAntiTest, AntiNullAsTrueWithNullsBasic) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_r, this->_table_wrapper_r, {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals},
      JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
}

TYPED_TEST(JoinSemiAntiTest, AntiNullAsTrueWithNullsLarger) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
      "resources/test_data/tbl/join_operators/anti_null_as_true_result_a.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
      "resources/test_data/tbl/join_operators/anti_null_as_true_result_b.tbl");
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_int_int_string_nulls_random, this->_table_wrapper_int_int_string_nulls_random,
      {{ColumnID{0}, ColumnID{2}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
      "resources/test_data/tbl/join_operators/anti_null_as_true_result_c.tbl");
}

}  // namespace opossum
