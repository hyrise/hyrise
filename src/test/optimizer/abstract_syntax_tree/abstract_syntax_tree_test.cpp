#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", 0));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float2.tbl", 0));
  }

  void TearDown() override { StorageManager::get().reset(); }
};

TEST_F(AbstractSyntaxTreeTest, ParentTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_EQ(table_node->parent(), nullptr);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parent(), predicate_node);
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parent(), projection_node);
  ASSERT_EQ(projection_node->left_child(), predicate_node);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_EQ(projection_node->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ClearParentTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parent(), predicate_node);
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);

  table_node->clear_parent();

  ASSERT_EQ(table_node->parent(), nullptr);
  ASSERT_EQ(predicate_node->left_child(), nullptr);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_EQ(table_node->parent(), nullptr);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpEquals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parent(), predicate_node);
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);

  const auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpEquals, "b");
  predicate_node_2->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parent(), predicate_node_2);
  ASSERT_EQ(predicate_node_2->left_child(), predicate_node);
  ASSERT_EQ(predicate_node_2->right_child(), nullptr);
  ASSERT_EQ(predicate_node_2->parent(), nullptr);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_2);

  ASSERT_EQ(predicate_node_2->parent(), projection_node);
  ASSERT_EQ(projection_node->left_child(), predicate_node_2);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_EQ(projection_node->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, TwoInputsTest) {
  const auto join_node = std::make_shared<JoinNode>(
      JoinMode::Inner, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);

  ASSERT_EQ(join_node->left_child(), nullptr);
  ASSERT_EQ(join_node->right_child(), nullptr);
  ASSERT_EQ(join_node->parent(), nullptr);

  const auto table_a_node = std::make_shared<StoredTableNode>("a");
  const auto table_b_node = std::make_shared<StoredTableNode>("b");

  join_node->set_left_child(table_a_node);
  join_node->set_right_child(table_b_node);

  ASSERT_EQ(join_node->left_child(), table_a_node);
  ASSERT_EQ(join_node->right_child(), table_b_node);
  ASSERT_EQ(join_node->parent(), nullptr);

  ASSERT_EQ(table_a_node->parent(), join_node);
  ASSERT_EQ(table_b_node->parent(), join_node);
}

}  // namespace opossum
