#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <storage/storage_manager.hpp>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "optimizer/strategy/join_detection_rule.hpp"

namespace opossum {

class JoinDetectionRuleTest : public BaseTest {
 protected:
  void SetUp() override {

    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("c", load_table("src/test/tables/int_float.tbl", 2));

    _table_node_a = std::make_shared<StoredTableNode>("a");
    _table_node_b = std::make_shared<StoredTableNode>("b");
    _table_node_c = std::make_shared<StoredTableNode>("c");
  }

  std::shared_ptr<StoredTableNode> _table_node_a, _table_node_b, _table_node_c;
  JoinConditionDetectionRule _rule;
};

TEST_F(JoinDetectionRuleTest, SimpleDetectionTest) {
  // Generate AST
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(cross_join_node);

  auto output = _rule.apply_to(predicate_node);

  EXPECT_EQ(output->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, SecondDetectionTest) {
  // Generate AST
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoPredicate) {
  // Generate AST
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(cross_join_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoMatchingPredicate) {
  // Generate AST
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, ColumnID{1});
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Predicate);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}


TEST_F(JoinDetectionRuleTest, NonCrossJoin) {
  const auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{1}, ColumnID{3}),
                                                    ScanType::OpEquals);
  join_node->set_left_child(_table_node_a);
  join_node->set_right_child(_table_node_b);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, ColumnID{3});
  predicate_node->set_left_child(join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Predicate);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, MultipleJoins) {
  const auto join_node1 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node1->set_left_child(_table_node_a);
  join_node1->set_right_child(_table_node_b);

  const auto join_node2 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node2->set_left_child(join_node1);
  join_node2->set_right_child(_table_node_c);

  const auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, ColumnID{2});
  predicate_node->set_left_child(join_node2);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = _rule.apply_to(projection_node);

  EXPECT_EQ(output->type(), ASTNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->type(), ASTNodeType::Join);
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), ASTNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), ASTNodeType::StoredTable);
}

}  // namespace opossum