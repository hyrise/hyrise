#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "optimizer/strategy/join_detection_rule.hpp"

namespace opossum {

class StoredTableNodeMock : public StoredTableNode {
 public:
  StoredTableNodeMock(const std::string &table_name, const std::vector<std::string> &column_names)
      : StoredTableNode(table_name) {
    _output_column_names = column_names;
  }

  explicit StoredTableNodeMock(const std::string &table_name) : StoredTableNode(table_name) {}
};

class JoinDetectionRuleTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_node_a = std::make_shared<StoredTableNodeMock>("a", std::vector<std::string>({"c1", "c2"}));
    _table_node_b = std::make_shared<StoredTableNodeMock>("b", std::vector<std::string>({"c1", "c2", "c3"}));
    _table_node_c = std::make_shared<StoredTableNodeMock>("c", std::vector<std::string>({"c1"}));
  }

  std::shared_ptr<StoredTableNode> _table_node_a, _table_node_b, _table_node_c;
  JoinConditionDetectionRule _rule;
};

TEST_F(JoinDetectionRuleTest, SimpleDetectionTest) {
  // Generate AST
  // There should not be a ScanType
  const auto cross_join_node = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto column_ref_left = Expression::create_column_reference("c1", "a");
  const auto column_ref_right = Expression::create_column_reference("c1", "b");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left, column_ref_right);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, join_condition, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(cross_join_node);

  auto output = _rule.apply_to(predicate_node);

  EXPECT_EQ(output->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, SecondDetectionTest) {
  // Generate AST
  // There should not be a ScanType
  const auto cross_join_node = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto column_ref_left = Expression::create_column_reference("c1", "a");
  const auto column_ref_right = Expression::create_column_reference("c1", "b");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left, column_ref_right);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, join_condition, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoPredicate) {
  // Generate AST
  // There should not be a ScanType
  const auto cross_join_node = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(cross_join_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoMatchingPredicate) {
  // Generate AST
  // There should not be a ScanType
  const auto cross_join_node = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto column_ref_left = Expression::create_column_reference("c1", "c");
  const auto column_ref_right = Expression::create_column_reference("c1", "b");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left, column_ref_right);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, join_condition, ScanType::OpEquals, 0);
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Predicate);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, ConditionIsPartOfPredicate) {
  // Generate AST
  // There should not be a ScanType
  const auto cross_join_node = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  // Setting up nested Join Condition
  const auto column_ref_left = Expression::create_column_reference("c2", "a");
  const auto literal = Expression::create_literal(5);
  const auto less_than = Expression::create_binary_operator(ExpressionType::LessThan, column_ref_left, literal);

  const auto column_ref_left_2 = Expression::create_column_reference("c1", "a");
  const auto column_ref_right = Expression::create_column_reference("c1", "b");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left_2, column_ref_right);

  const auto and_condition = Expression::create_binary_operator(ExpressionType::And, less_than, join_condition);

  //
  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, and_condition, ScanType::OpEquals, 0);
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Predicate);
  const auto returned_predicate_node = std::dynamic_pointer_cast<PredicateNode>(output->left_child());
  EXPECT_EQ(returned_predicate_node->predicate()->type(), ExpressionType::And);

  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  const auto returned_join_node = std::dynamic_pointer_cast<JoinNode>(output->left_child()->left_child());
  EXPECT_EQ(returned_join_node->join_mode(), JoinMode::Inner);

  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NonCrossJoin) {
  const auto join_node = std::make_shared<JoinNode>(std::make_pair(std::string("c2"), std::string("c2")),
                                                    ScanType::OpEquals, JoinMode::Inner);
  join_node->set_left_child(_table_node_a);
  join_node->set_right_child(_table_node_b);

  const auto column_ref_left = Expression::create_column_reference("c1", "a");
  const auto column_ref_right = Expression::create_column_reference("c1", "b");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left, column_ref_right);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, join_condition, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(join_node);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Predicate);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

// TODO(Sven): Enable after ColumnIDs have been merged
TEST_F(JoinDetectionRuleTest, DISABLED_MultipleJoins) {
  const auto join_node1 = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  join_node1->set_left_child(_table_node_a);
  join_node1->set_right_child(_table_node_b);

  const auto join_node2 = std::make_shared<JoinNode>(nullopt, ScanType::OpEquals, JoinMode::Cross);
  join_node2->set_left_child(join_node1);
  join_node2->set_right_child(_table_node_c);

  // Setting up nested Join Condition
  const auto column_ref_left = Expression::create_column_reference("c1", "a");
  const auto column_ref_right = Expression::create_column_reference("c1", "c");
  const auto join_condition =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left, column_ref_right);

  const auto column_ref_left2 = Expression::create_column_reference("c2", "a");
  const auto column_ref_right2 = Expression::create_column_reference("c2", "b");
  const auto join_condition2 =
      Expression::create_binary_operator(ExpressionType::Equals, column_ref_left2, column_ref_right2);

  const auto and_condition =
      Expression::create_binary_operator(ExpressionType::And, join_condition, join_condition2);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{}, and_condition, ScanType::OpEquals, ColumnID{});
  predicate_node->set_left_child(join_node2);

  const std::vector<std::string> column_names = {"c1"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

}  // namespace opossum