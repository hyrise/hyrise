#include <iostream>
#include <memory>
#include <optional>
#include <string>

#include "base_test.hpp"
#include "join_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/operator_join_predicate.hpp"

namespace opossum {

// This file contains tests for multi predicate joins

struct TablePair {
  std::shared_ptr<TableWrapper> left;
  std::shared_ptr<TableWrapper> right;
};

struct JoinParameters {
  JoinMode join_mode;
  TablePair table_pair;
  OperatorJoinPredicate first_predicate;
  std::string expected_result_table_file_path;
  size_t chunk_size;
  std::vector<OperatorJoinPredicate> additional_predicates;
};

template <typename JoinType>
class JoinMultiPredicateTest : public JoinTest {
 public:
  // called once before the first test case is executed
  static void SetUpTestCase() {
    _table_wrapper_a = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/int_int_string_a.tbl", 2));
    _table_wrapper_a_nulls_first = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/int_int_string_nulls_first_a.tbl", 2));
    _table_wrapper_a_nulls_last = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/int_int_string_nulls_last_a.tbl", 2));
    _table_wrapper_a_nulls_random = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/int_int_string_nulls_random_a.tbl", 2));
    _table_wrapper_a2_nulls_random = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/int_string_string_nulls_random_a2.tbl", 2));
    _table_wrapper_b_larger = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/string_int_int_b_larger.tbl", 2));
    _table_wrapper_b_nulls_first_larger = std::make_shared<TableWrapper>(load_table(
        "resources/test_data/tbl/join_operators/multi_predicates/string_int_int_nulls_first_b_larger.tbl", 2));
    _table_wrapper_b_nulls_last_larger = std::make_shared<TableWrapper>(load_table(
        "resources/test_data/tbl/join_operators/multi_predicates/string_int_int_nulls_last_b_larger.tbl", 2));
    _table_wrapper_b_nulls_random = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/multi_predicates/string_int_int_nulls_random_b.tbl", 2));
    _table_wrapper_b_nulls_random_larger = std::make_shared<TableWrapper>(load_table(
        "resources/test_data/tbl/join_operators/multi_predicates/string_int_int_nulls_random_b_larger.tbl", 2));
    _table_wrapper_b2_nulls_random_larger = std::make_shared<TableWrapper>(load_table(
        "resources/test_data/tbl/join_operators/multi_predicates/string_string_int_nulls_random_b2_larger.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_a_nulls_first->execute();
    _table_wrapper_a_nulls_last->execute();
    _table_wrapper_a_nulls_random->execute();
    _table_wrapper_a2_nulls_random->execute();
    _table_wrapper_b_larger->execute();
    _table_wrapper_b_nulls_first_larger->execute();
    _table_wrapper_b_nulls_last_larger->execute();
    _table_wrapper_b_nulls_random->execute();
    _table_wrapper_b_nulls_random_larger->execute();
    _table_wrapper_b2_nulls_random_larger->execute();

    // setup base choice (see input domain modeling specification below the class definition)
    _base_choice_join_parameters =
        JoinParameters{JoinMode::Inner,
                       TablePair{_table_wrapper_a_nulls_random, _table_wrapper_b_nulls_random_larger},
                       OperatorJoinPredicate{_column_pair_1, PredicateCondition::Equals},
                       "resources/test_data/tbl/join_operators/multi_predicates/"
                       "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_gt.tbl",
                       2,
                       {{_column_pair_2, PredicateCondition::GreaterThan}}};
  }

 protected:
  void SetUp() override { JoinTest::SetUp(); }

  void _test_join_output(const JoinParameters params) {
    test_join_output<JoinType>(params.table_pair.left, params.table_pair.right, params.first_predicate.column_ids,
                               params.first_predicate.predicate_condition, params.join_mode,
                               params.expected_result_table_file_path, params.chunk_size, params.additional_predicates);
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper_a;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_a_nulls_first;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_a_nulls_last;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_a_nulls_random;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_a2_nulls_random;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b_larger;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b_nulls_first_larger;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b_nulls_last_larger;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b_nulls_random;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b_nulls_random_larger;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_b2_nulls_random_larger;

  inline static ColumnIDPair _column_pair_1{ColumnID{0}, ColumnID{2}};
  inline static ColumnIDPair _column_pair_2{ColumnID{1}, ColumnID{1}};
  inline static ColumnIDPair _column_pair_3{ColumnID{2}, ColumnID{0}};

  inline static std::optional<JoinParameters> _base_choice_join_parameters;
};

// Input Domain Modeling

// multi predicate join characteristics
// ---------------
// [A] join mode
//    [1] Inner
//    [2] Left
//    [3] Right
//    [4] Full outer
//    [5] Cross
//    [6] Semi
//    [7] Anti
// [B] predicate condition used at least once
//    [1] GreaterThan
//    [2] GreaterThanEquals
//    [3] LessThanEquals
//    [4] LessThan
//    [5] Equals
//    [6] NotEquals
// [C] null values in value comparisons
//    [1] left
//    [2] right
//    [3] both
//    [4] none
// [D] condition satisfaction
//    [1] none
//    [2] 1. predicate
//    [3] 1. & 2. predicate
//    [4] all predicates
// [E] null value distribution in columns
//    [1] random
//    [2] nulls last
//    [3] nulls first
// [F] size of join columns
//    [1] left > right
//    [2] left < right
//    [3] same sizes
// [G] number of additional predicates
//    [1] 1
//    [2] >1
// [H] data type relation between join columns
//    [1] equal
//    [2] not equal
// [I] column data types
//    [1] int
//    [2] string

// Choosing combinations using the Base Choice strategy:

// Base Choice:
// A1 B5 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqualsGreaterThan

// A variations:
// A2 B5 C2 D3 E1 F2 G1 H1 I1       LeftLTableSmallerRTableRandomNullsEqGt
// A3 B5 C2 D3 E1 F2 G1 H1 I1       RightLTableSmallerRTableRandomNullsEqGt
// A4 B5 C2 D3 E1 F2 G1 H1 I1       OuterLTableSmallerRTableRandomNullsEqGt
// A5 B5 C2 D3 E1 F2 G1 H1 I1       CrossLTableSmallerRTableRandomNullsEqGt
// A6 B5 C2 D3 E1 F2 G1 H1 I1       SemiLTableSmallerRTableRandomNullsEqGt
// A7 B5 C2 D3 E1 F2 G1 H1 I1       AntiLTableSmallerRTableRandomNullsEqGt
// B variations:
// A1 B1 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// A1 B2 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGte
// A1 B3 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqLte
// A1 B4 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqLt
// A1 B6 C2 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqNe
// C variations:
// A1 B5 C1 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// A1 B5 C3 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// A1 B5 C4 D3 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// D variations:
// A1 B5 C2 D1 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// A1 B5 C2 D2 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// A1 B5 C2 D4 E1 F2 G1 H1 I1       InnerLTableSmallerRTableRandomNullsEqGt
// E variations:
// A1 B5 C2 D3 E2 F2 G1 H1 I1       InnerLTableSmallerRTableNullsLastEqGt
// A1 B5 C2 D3 E3 F2 G1 H1 I1       InnerLTableSmallerRTableNullsFirstEqGt
// F variations:
// A1 B5 C2 D3 E1 F1 G1 H1 I1       InnerLTableLargerRTableRandomNullsEqGt
// A1 B5 C2 D3 E1 F3 G1 H1 I1       InnerLTableSameSizeRTableRandomNullsEqGt
// G variations:
// A1 B5 C2 D3 E1 F2 G2 H1 I1       InnerLTableSmallerRTableRandomNullsEqGtEq
// H variations:
// A1 B5 C2 D3 E1 F2 G1 H2 I1       InnerLTableSmallerRTableRandomNullsDifferentDataTypesEqGt
// I variations:
// A1 B5 C2 D3 E1 F2 G1 H1 I2       InnerLTableSmallerRTableRandomNullsStringComparisonEqGt

// TODO(anyone) add other Join types when they support multi predicates
using JoinMultiPredicateTypes = ::testing::Types<JoinHash>;
TYPED_TEST_CASE(JoinMultiPredicateTest, JoinMultiPredicateTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqGt) {
  this->_test_join_output(this->_base_choice_join_parameters.value());
}

TYPED_TEST(JoinMultiPredicateTest, LeftLTableSmallerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::Left;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_left_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, RightLTableSmallerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::Right;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_right_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, RightLTableLargerRTableRandomNullsEqGt) {
  // WIP: Implement test correctly
  GTEST_SKIP();
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::Right;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_right_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, OuterLTableSmallerRTableRandomNullsEqGt) {
  // HashJoin does not currently support outer joins
  GTEST_SKIP();
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::FullOuter;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_outer_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

// test case is in conflict with
// NOLINT https://github.com/hyrise/hyrise/blob/ca5f3093d27c5e29f7cefac783d48c853d32179e/src/lib/operators/abstract_join_operator.cpp#L19-L20
/*
TYPED_TEST(JoinMultiPredicateTest, CrossLTableSmallerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters;
  parameters.join_mode = JoinMode::Cross;
  this->_test_join_output(parameters);
}
*/

TYPED_TEST(JoinMultiPredicateTest, SemiLTableSmallerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::Semi;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_semi_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, AntiLTableSmallerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.join_mode = JoinMode::Anti;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_anti_a_nulls_random_b_nulls_random_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqGte) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.additional_predicates = {{this->_column_pair_2, PredicateCondition::GreaterThanEquals}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_gte.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqLte) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.additional_predicates = {{this->_column_pair_2, PredicateCondition::LessThanEquals}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_lte.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqLt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.additional_predicates = {{this->_column_pair_2, PredicateCondition::LessThan}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_lt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqNe) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.additional_predicates = {{this->_column_pair_2, PredicateCondition::NotEquals}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_ne.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableNullsLastEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.left = this->_table_wrapper_a_nulls_last;
  parameters.table_pair.right = this->_table_wrapper_b_nulls_last_larger;
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableNullsFirstEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.left = this->_table_wrapper_a_nulls_first;
  parameters.table_pair.right = this->_table_wrapper_b_nulls_first_larger;
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableLargerRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.left = parameters.table_pair.right;
  parameters.table_pair.right = this->_base_choice_join_parameters->table_pair.left;
  // swap column pairs of the predicates
  parameters.first_predicate.column_ids.first = this->_column_pair_1.second;
  parameters.first_predicate.column_ids.second = this->_column_pair_1.first;
  parameters.additional_predicates = {
      {ColumnIDPair{this->_column_pair_2.second, this->_column_pair_2.first}, PredicateCondition::GreaterThan}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_b_larger_nulls_rand_a_nulls_rand_eq_gt.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSameSizeRTableRandomNullsEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.right = this->_table_wrapper_b_nulls_random;
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsEqGtEq) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.additional_predicates = {{this->_column_pair_2, PredicateCondition::GreaterThan},
                                      {this->_column_pair_3, PredicateCondition::Equals}};
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b_nulls_rand_larger_eq_gt_eq.tbl";
  this->_test_join_output(parameters);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsDifferentDataTypesEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.right = this->_table_wrapper_b2_nulls_random_larger;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a_nulls_rand_b2_nulls_rand_eq_gt.tbl";
  // Throw logic error: comparison of different data types is not intended.
  EXPECT_THROW(this->_test_join_output(parameters), std::logic_error);
}

TYPED_TEST(JoinMultiPredicateTest, InnerLTableSmallerRTableRandomNullsStringComparisonEqGt) {
  auto parameters = this->_base_choice_join_parameters.value();
  parameters.table_pair.left = this->_table_wrapper_a2_nulls_random;
  parameters.table_pair.right = this->_table_wrapper_b2_nulls_random_larger;
  parameters.expected_result_table_file_path =
      "resources/test_data/tbl/join_operators/multi_predicates/"
      "result_inner_a2_nulls_rand_b2_nulls_rand_larger_eq_gt.tbl";
  this->_test_join_output(parameters);
}

}  // namespace opossum
