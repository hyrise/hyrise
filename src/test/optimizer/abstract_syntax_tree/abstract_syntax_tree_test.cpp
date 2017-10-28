#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/mock_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", 0));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float2.tbl", 0));

    /**
     * Init complex graph. This one is hard to visualize in ASCII. I suggest drawing it from the initialization below,
     * in case you need to visualize it
     */
    for (auto& node : _nodes) {
      node = std::make_shared<MockNode>();
    }

    _nodes[5]->set_right_child(_nodes[7]);
    _nodes[0]->set_right_child(_nodes[4]);
    _nodes[4]->set_left_child(_nodes[5]);
    _nodes[4]->set_right_child(_nodes[7]);
    _nodes[5]->set_left_child(_nodes[6]);
    _nodes[2]->set_left_child(_nodes[5]);
    _nodes[1]->set_right_child(_nodes[3]);
    _nodes[3]->set_left_child(_nodes[5]);
    _nodes[3]->set_right_child(_nodes[7]);
    _nodes[1]->set_left_child(_nodes[2]);
    _nodes[0]->set_left_child(_nodes[1]);
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::array<std::shared_ptr<MockNode>, 8> _nodes;
};

TEST_F(AbstractSyntaxTreeTest, SimpleParentTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_TRUE(table_node->parents().empty());

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{projection_node});
  ASSERT_EQ(projection_node->left_child(), predicate_node);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_TRUE(projection_node->parents().empty());
}

TEST_F(AbstractSyntaxTreeTest, SimpleClearParentsTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  table_node->clear_parents();

  ASSERT_TRUE(table_node->parents().empty());
  ASSERT_EQ(predicate_node->left_child(), nullptr);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_TRUE(table_node->parents().empty());

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  const auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpEquals, "b");
  predicate_node_2->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{predicate_node_2});
  ASSERT_EQ(predicate_node_2->left_child(), predicate_node);
  ASSERT_EQ(predicate_node_2->right_child(), nullptr);
  ASSERT_TRUE(predicate_node_2->parents().empty());

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_2);

  ASSERT_EQ(predicate_node_2->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{projection_node});
  ASSERT_EQ(projection_node->left_child(), predicate_node_2);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_TRUE(projection_node->parents().empty());
}

TEST_F(AbstractSyntaxTreeTest, TwoInputsTest) {
  const auto join_node = std::make_shared<JoinNode>(
      JoinMode::Inner, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);

  ASSERT_EQ(join_node->left_child(), nullptr);
  ASSERT_EQ(join_node->right_child(), nullptr);
  ASSERT_TRUE(join_node->parents().empty());

  const auto table_a_node = std::make_shared<StoredTableNode>("a");
  const auto table_b_node = std::make_shared<StoredTableNode>("b");

  join_node->set_left_child(table_a_node);
  join_node->set_right_child(table_b_node);

  ASSERT_EQ(join_node->left_child(), table_a_node);
  ASSERT_EQ(join_node->right_child(), table_b_node);
  ASSERT_TRUE(join_node->parents().empty());

  ASSERT_EQ(table_a_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{join_node});
  ASSERT_EQ(table_b_node->parents(), std::vector<std::shared_ptr<AbstractASTNode>>{join_node});
}

TEST_F(AbstractSyntaxTreeTest, AliasedSubqueryTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");
  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);
  predicate_node->set_alias(std::string("foo"));

  ASSERT_TRUE(predicate_node->knows_table("foo"));
  ASSERT_FALSE(predicate_node->knows_table("a"));

  ASSERT_EQ(predicate_node->get_column_id_by_named_column_reference({"b"}), ColumnID{1});
  ASSERT_EQ(predicate_node->get_column_id_by_named_column_reference({"b", {"foo"}}), ColumnID{1});
  ASSERT_EQ(predicate_node->find_column_id_by_named_column_reference({"b", {"a"}}), std::nullopt);
}

TEST_F(AbstractSyntaxTreeTest, ComplexGraphStructure) {
  ASSERT_AST_TIE(_nodes[0], ASTChildSide::Left, _nodes[1]);
  ASSERT_AST_TIE(_nodes[0], ASTChildSide::Right, _nodes[4]);
  ASSERT_AST_TIE(_nodes[1], ASTChildSide::Left, _nodes[2]);
  ASSERT_AST_TIE(_nodes[1], ASTChildSide::Right, _nodes[3]);
  ASSERT_AST_TIE(_nodes[2], ASTChildSide::Left, _nodes[5]);
  ASSERT_AST_TIE(_nodes[3], ASTChildSide::Left, _nodes[5]);
  ASSERT_AST_TIE(_nodes[4], ASTChildSide::Left, _nodes[5]);
  ASSERT_AST_TIE(_nodes[3], ASTChildSide::Right, _nodes[7]);
  ASSERT_AST_TIE(_nodes[5], ASTChildSide::Left, _nodes[6]);
  ASSERT_AST_TIE(_nodes[5], ASTChildSide::Right, _nodes[7]);
  ASSERT_AST_TIE(_nodes[4], ASTChildSide::Right, _nodes[7]);
}

TEST_F(AbstractSyntaxTreeTest, ComplexGraphRemoveFromTree) {
  _nodes[2]->remove_from_tree();

  EXPECT_TRUE(_nodes[2]->parents().empty());
  EXPECT_EQ(_nodes[2]->left_child(), nullptr);
  EXPECT_EQ(_nodes[2]->right_child(), nullptr);

  ASSERT_AST_TIE(_nodes[1], ASTChildSide::Left, _nodes[5]);

  // Those ties should continue to exist
  ASSERT_AST_TIE(_nodes[1], ASTChildSide::Right, _nodes[3]);
  ASSERT_AST_TIE(_nodes[3], ASTChildSide::Left, _nodes[5]);
  ASSERT_AST_TIE(_nodes[4], ASTChildSide::Left, _nodes[5]);
}

TEST_F(AbstractSyntaxTreeTest, ComplexGraphReplaceInTree) {
  auto new_node = std::make_shared<MockNode>();

  new_node->replace_in_tree(_nodes[5]);

  ASSERT_AST_TIE(_nodes[2], ASTChildSide::Left, new_node);
  ASSERT_AST_TIE(_nodes[3], ASTChildSide::Left, new_node);
  ASSERT_AST_TIE(_nodes[4], ASTChildSide::Left, new_node);
  ASSERT_AST_TIE(new_node, ASTChildSide::Left, _nodes[6]);
  ASSERT_AST_TIE(new_node, ASTChildSide::Right, _nodes[7]);
}

}  // namespace opossum
