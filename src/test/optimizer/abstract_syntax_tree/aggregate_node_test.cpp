#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class AggregateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_int_int.tbl", 0));

    _stored_table_node = std::make_shared<StoredTableNode>("t_a");

    // SELECT a, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = std::make_shared<AggregateNode>(
        std::vector<std::shared_ptr<ExpressionNode>>{
            ExpressionNode::create_function_reference(
                "SUM", {ExpressionNode::create_binary_operator(ExpressionType::Addition,
                                                               ExpressionNode::create_column_identifier(ColumnID{0}),
                                                               ExpressionNode::create_column_identifier(ColumnID{1}))}),
            ExpressionNode::create_function_reference(
                "SUM",
                {ExpressionNode::create_binary_operator(ExpressionType::Addition,
                                                        ExpressionNode::create_column_identifier(ColumnID{0}),
                                                        ExpressionNode::create_column_identifier(ColumnID{2}))},
                {std::string("some_sum")})},
        std::vector<ColumnID>{ColumnID{0}});
    _aggregate_node->set_left_child(_stored_table_node);
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
};

TEST_F(AggregateNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_aggregate_node->get_column_id_for_column_identifier_name({"a", nullopt}), 0);
  EXPECT_EQ(_aggregate_node->get_column_id_for_column_identifier_name({"a", {"t_a"}}), 0);
  EXPECT_EQ(_aggregate_node->find_column_id_for_column_identifier_name({"b", nullopt}), nullopt);
  EXPECT_EQ(_aggregate_node->find_column_id_for_column_identifier_name({"b", {"t_a"}}), nullopt);

  EXPECT_EQ(_aggregate_node->get_column_id_for_column_identifier_name({"some_sum", nullopt}), 2);
  EXPECT_EQ(_aggregate_node->find_column_id_for_column_identifier_name({"some_sum", {"t_a"}}), nullopt);
}

TEST_F(AggregateNodeTest, ColumnIdForExpression) {
  EXPECT_EQ(_aggregate_node->get_column_id_for_expression(
    ExpressionNode::create_column_identifier(ColumnID{0})
  ), 0);

  // There is no a+b
  EXPECT_EQ(_aggregate_node->find_column_id_for_expression(ExpressionNode::create_binary_operator(
                ExpressionType::Addition, ExpressionNode::create_column_identifier(ColumnID{0}),
                ExpressionNode::create_column_identifier(ColumnID{1}))),
            nullopt);

  // But there is SUM(a+b)
  EXPECT_EQ(_aggregate_node->find_column_id_for_expression(ExpressionNode::create_function_reference(
                "SUM", {ExpressionNode::create_binary_operator(
                           ExpressionType::Addition, ExpressionNode::create_column_identifier(ColumnID{0}),
                           ExpressionNode::create_column_identifier(ColumnID{2}))})),
            nullopt);

  EXPECT_EQ(_aggregate_node->find_column_id_for_expression(ExpressionNode::create_binary_operator(
                ExpressionType::Addition, ExpressionNode::create_column_identifier(ColumnID{1}),
                ExpressionNode::create_column_identifier(ColumnID{1}))),
            nullopt);

  // TODO(mp) enable once this is done independent of alias
  //  EXPECT_EQ(_aggregate_node->get_column_id_for_expression(
  //    ExpressionNode::create_binary_operator(
  //      ExpressionType::Addition,
  //      ExpressionNode::create_column_identifier(ColumnID{0}),
  //      ExpressionNode::create_column_identifier(ColumnID{2})
  //    )
  //  ), 1);
}

}  // namespace opossum
