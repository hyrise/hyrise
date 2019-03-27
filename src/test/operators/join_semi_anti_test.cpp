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
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Semi-, AntiNullAsTrue and AntiRetainsNull-Join implementations.
*/

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

TEST_F(JoinSemiAntiTest, SemiJoin) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiJoinRefSegments) {
  auto scan_a = this->create_table_scan(_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiJoinBig) {
  test_join_output<JoinHash>(_table_wrapper_semi_a, _table_wrapper_semi_b,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/join_operators/semi_result.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoinWithoutNulls) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/anti_int4.tbl");
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/anti_int4.tbl");
}

TEST_F(JoinSemiAntiTest, AntiJoinWithoutNullsJoinRefSegments) {
  auto scan_a = this->create_table_scan(_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::AntiNullAsTrue, "resources/test_data/tbl/join_operators/anti_int4.tbl", 1);
  test_join_output<JoinHash>(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::AntiNullAsFalse, "resources/test_data/tbl/join_operators/anti_int4.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiWithoutNullsBig) {
  test_join_output<JoinHash>(_table_wrapper_semi_a, _table_wrapper_semi_b,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/anti_result.tbl", 1);
  test_join_output<JoinHash>(_table_wrapper_semi_a, _table_wrapper_semi_b,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/anti_result.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiWithNulls) {
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Semi,
                             "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
}

TEST_F(JoinSemiAntiTest, AntiNullAsFalseWithNullsBasic) {
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/int_int_with_zero_and_null.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
}

TEST_F(JoinSemiAntiTest, AntiNullAsFalseWithNullsLarger) {
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/anti_null_as_false_result_a.tbl");
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/anti_null_as_false_result_b.tbl");
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{2}}, PredicateCondition::Equals}, JoinMode::AntiNullAsFalse,
                             "resources/test_data/tbl/join_operators/anti_null_as_false_result_c.tbl");
}

TEST_F(JoinSemiAntiTest, AntiNullAsTrueWithNullsBasic) {
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
  test_join_output<JoinHash>(_table_wrapper_r, _table_wrapper_r,
                             {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/int_int_null_empty.tbl");
}

TEST_F(JoinSemiAntiTest, AntiNullAsTrueWithNullsLarger) {
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/anti_null_as_true_result_a.tbl");
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/anti_null_as_true_result_b.tbl");
  test_join_output<JoinHash>(_table_wrapper_int_int_string_nulls_random, _table_wrapper_int_int_string_nulls_random,
                             {{ColumnID{0}, ColumnID{2}}, PredicateCondition::Equals}, JoinMode::AntiNullAsTrue,
                             "resources/test_data/tbl/join_operators/anti_null_as_true_result_c.tbl");
}

}  // namespace opossum
