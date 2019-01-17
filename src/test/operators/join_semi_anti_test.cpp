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
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Semi- and Anti-Join implementations.
*/

class JoinSemiAntiTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_semi_a =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/semi_left.tbl", 2));
    _table_wrapper_semi_b =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/semi_right.tbl", 2));

    _table_wrapper_semi_a->execute();
    _table_wrapper_semi_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_semi_a, _table_wrapper_semi_b;
};

TEST_F(JoinSemiAntiTest, SemiJoin) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals,
                             JoinMode::Semi, "resources/test_data/tbl/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiJoinRefSegments) {
  auto scan_a = this->create_table_scan(_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals, JoinMode::Semi,
                             "resources/test_data/tbl/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiJoinBig) {
  test_join_output<JoinHash>(_table_wrapper_semi_a, _table_wrapper_semi_b, {ColumnID{0}, ColumnID{0}},
                             PredicateCondition::Equals, JoinMode::Semi,
                             "resources/test_data/tbl/joinoperators/semi_result.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoin) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals,
                             JoinMode::Anti, "resources/test_data/tbl/joinoperators/anti_int4.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoinRefSegments) {
  auto scan_a = this->create_table_scan(_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals, JoinMode::Anti,
                             "resources/test_data/tbl/joinoperators/anti_int4.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoinBig) {
  test_join_output<JoinHash>(_table_wrapper_semi_a, _table_wrapper_semi_b, {ColumnID{0}, ColumnID{0}},
                             PredicateCondition::Equals, JoinMode::Anti,
                             "resources/test_data/tbl/joinoperators/anti_result.tbl", 1);
}

}  // namespace opossum
