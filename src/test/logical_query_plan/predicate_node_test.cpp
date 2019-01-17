#include <memory>

#include "base_test.hpp"
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
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("resources/test_data/tbl/int_float_double_string.tbl", 2));

    _table_node = StoredTableNode::make("table_a");
    _i = {_table_node, ColumnID{0}};
    _f = {_table_node, ColumnID{1}};

    _predicate_node = PredicateNode::make(equals_(_i, 5), _table_node);
  }

  std::shared_ptr<StoredTableNode> _table_node;
  LQPColumnReference _i, _f;
  std::shared_ptr<PredicateNode> _predicate_node;
};

TEST_F(PredicateNodeTest, Descriptions) { EXPECT_EQ(_predicate_node->description(), "[Predicate] i = 5"); }

TEST_F(PredicateNodeTest, Equals) {
  EXPECT_EQ(*_predicate_node, *_predicate_node);

  const auto other_predicate_node_a = PredicateNode::make(equals_(_i, 5), _table_node);
  const auto other_predicate_node_b = PredicateNode::make(equals_(_f, 5), _table_node);
  const auto other_predicate_node_c = PredicateNode::make(not_equals_(_i, 5), _table_node);
  const auto other_predicate_node_d = PredicateNode::make(equals_(_i, 6), _table_node);

  EXPECT_EQ(*other_predicate_node_a, *_predicate_node);
  EXPECT_NE(*other_predicate_node_b, *_predicate_node);
  EXPECT_NE(*other_predicate_node_c, *_predicate_node);
  EXPECT_NE(*other_predicate_node_d, *_predicate_node);
}

TEST_F(PredicateNodeTest, Copy) { EXPECT_EQ(*_predicate_node->deep_copy(), *_predicate_node); }

TEST_F(PredicateNodeTest, NodeExpressions) {
  ASSERT_EQ(_predicate_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_predicate_node->node_expressions.at(0), *equals_(_i, 5));
}

}  // namespace opossum
