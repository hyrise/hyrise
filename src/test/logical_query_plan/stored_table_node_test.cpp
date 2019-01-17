#include <memory>
#include <string>

#include "gtest/gtest.h"

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
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("resources/test_data/tbl/int_float.tbl"));
    StorageManager::get().add_table("t_b", load_table("resources/test_data/tbl/int_float.tbl"));

    _stored_table_node = StoredTableNode::make("t_a");
    _a = LQPColumnReference(_stored_table_node, ColumnID{0});
    _b = LQPColumnReference(_stored_table_node, ColumnID{1});

    _stored_table_node->set_excluded_chunk_ids({ChunkID{2}});
  }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  LQPColumnReference _a, _b;
};

TEST_F(StoredTableNodeTest, Description) { EXPECT_EQ(_stored_table_node->description(), "[StoredTable] Name: 't_a'"); }

TEST_F(StoredTableNodeTest, GetColumn) {
  EXPECT_EQ(_stored_table_node->get_column("a"), _a);
  EXPECT_EQ(_stored_table_node->get_column("b"), _b);
}

TEST_F(StoredTableNodeTest, Equals) {
  EXPECT_EQ(*_stored_table_node, *_stored_table_node);

  const auto different_node_a = StoredTableNode::make("t_b");
  different_node_a->set_excluded_chunk_ids({ChunkID{2}});

  const auto different_node_b = StoredTableNode::make("t_a");

  EXPECT_NE(*_stored_table_node, *different_node_a);
  EXPECT_NE(*_stored_table_node, *different_node_b);
}

TEST_F(StoredTableNodeTest, Copy) { EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node); }

TEST_F(StoredTableNodeTest, NodeExpressions) { ASSERT_EQ(_stored_table_node->node_expressions.size(), 0u); }

}  // namespace opossum
