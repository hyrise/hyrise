#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class LogicalQueryPlanTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));

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

  std::array<std::shared_ptr<MockNode>, 8> _nodes;
};

TEST_F(LogicalQueryPlanTest, SimpleParentTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_TRUE(table_node->parents().empty());

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{projection_node});
  ASSERT_EQ(projection_node->left_child(), predicate_node);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_TRUE(projection_node->parents().empty());

  ASSERT_ANY_THROW(projection_node->get_child_side(table_node));
}

TEST_F(LogicalQueryPlanTest, SimpleClearParentsTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  table_node->clear_parents();

  ASSERT_TRUE(table_node->parents().empty());
  ASSERT_EQ(predicate_node->left_child(), nullptr);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());
}

TEST_F(LogicalQueryPlanTest, ChainSameNodesTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");

  ASSERT_EQ(table_node->left_child(), nullptr);
  ASSERT_EQ(table_node->right_child(), nullptr);
  ASSERT_TRUE(table_node->parents().empty());

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, "a");
  predicate_node->set_left_child(table_node);

  ASSERT_EQ(table_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{predicate_node});
  ASSERT_EQ(predicate_node->left_child(), table_node);
  ASSERT_EQ(predicate_node->right_child(), nullptr);
  ASSERT_TRUE(predicate_node->parents().empty());

  const auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::Equals, "b");
  predicate_node_2->set_left_child(predicate_node);

  ASSERT_EQ(predicate_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{predicate_node_2});
  ASSERT_EQ(predicate_node_2->left_child(), predicate_node);
  ASSERT_EQ(predicate_node_2->right_child(), nullptr);
  ASSERT_TRUE(predicate_node_2->parents().empty());

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_2);

  ASSERT_EQ(predicate_node_2->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{projection_node});
  ASSERT_EQ(projection_node->left_child(), predicate_node_2);
  ASSERT_EQ(projection_node->right_child(), nullptr);
  ASSERT_TRUE(projection_node->parents().empty());
}

TEST_F(LogicalQueryPlanTest, TwoInputsTest) {
  const auto join_node = std::make_shared<JoinNode>(
      JoinMode::Inner, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}), ScanType::Equals);

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

  ASSERT_EQ(table_a_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{join_node});
  ASSERT_EQ(table_b_node->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{join_node});
}

TEST_F(LogicalQueryPlanTest, AliasedSubqueryTest) {
  const auto table_node = std::make_shared<StoredTableNode>("a");
  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, "a");
  predicate_node->set_left_child(table_node);
  predicate_node->set_alias(std::string("foo"));

  ASSERT_TRUE(predicate_node->knows_table("foo"));
  ASSERT_FALSE(predicate_node->knows_table("a"));

  ASSERT_EQ(predicate_node->get_column_id_by_named_column_reference({"b"}), ColumnID{1});
  ASSERT_EQ(predicate_node->get_column_id_by_named_column_reference({"b", {"foo"}}), ColumnID{1});
  ASSERT_EQ(predicate_node->find_column_id_by_named_column_reference({"b", {"a"}}), std::nullopt);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphStructure) {
  ASSERT_LQP_TIE(_nodes[0], LQPChildSide::Left, _nodes[1]);
  ASSERT_LQP_TIE(_nodes[0], LQPChildSide::Right, _nodes[4]);
  ASSERT_LQP_TIE(_nodes[1], LQPChildSide::Left, _nodes[2]);
  ASSERT_LQP_TIE(_nodes[1], LQPChildSide::Right, _nodes[3]);
  ASSERT_LQP_TIE(_nodes[2], LQPChildSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[5], LQPChildSide::Left, _nodes[6]);
  ASSERT_LQP_TIE(_nodes[5], LQPChildSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Right, _nodes[7]);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphPrinted) {
  std::stringstream stream;
  _nodes[0]->print(stream);

  ASSERT_EQ(stream.str(), R"([0] [MockTable]
 \_[1] [MockTable]
 |  \_[2] [MockTable]
 |  |  \_[3] [MockTable]
 |  |     \_[4] [MockTable]
 |  |     \_[5] [MockTable]
 |  \_[6] [MockTable]
 |     \_Recurring Node --> [3]
 |     \_Recurring Node --> [5]
 \_[7] [MockTable]
    \_Recurring Node --> [3]
    \_Recurring Node --> [5]
)");
}

TEST_F(LogicalQueryPlanTest, ComplexGraphRemoveFromTree) {
  _nodes[2]->remove_from_tree();

  EXPECT_TRUE(_nodes[2]->parents().empty());
  EXPECT_EQ(_nodes[2]->left_child(), nullptr);
  EXPECT_EQ(_nodes[2]->right_child(), nullptr);

  // Make sure _node[1], _node[3] and _node[4] are the only parents _nodes[5] has
  EXPECT_EQ(_nodes[5]->parents().size(), 3u);
  ASSERT_LQP_TIE(_nodes[1], LQPChildSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Left, _nodes[5]);

  ASSERT_LQP_TIE(_nodes[1], LQPChildSide::Right, _nodes[3]);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphRemoveFromTreeLeaf) {
  _nodes[6]->remove_from_tree();
  _nodes[7]->remove_from_tree();

  EXPECT_TRUE(_nodes[6]->parents().empty());
  EXPECT_TRUE(_nodes[7]->parents().empty());
  EXPECT_EQ(_nodes[6]->left_child(), nullptr);
  EXPECT_EQ(_nodes[6]->right_child(), nullptr);
  EXPECT_EQ(_nodes[7]->left_child(), nullptr);
  EXPECT_EQ(_nodes[7]->right_child(), nullptr);
  EXPECT_EQ(_nodes[5]->left_child(), nullptr);
  EXPECT_EQ(_nodes[3]->right_child(), nullptr);
  EXPECT_EQ(_nodes[4]->right_child(), nullptr);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphReplaceWith) {
  auto new_node = std::make_shared<MockNode>();

  _nodes[5]->replace_with(new_node);

  // Make sure _nodes[5] is untied from the LQP
  EXPECT_TRUE(_nodes[5]->parents().empty());
  EXPECT_EQ(_nodes[5]->left_child(), nullptr);
  EXPECT_EQ(_nodes[5]->right_child(), nullptr);

  // Make sure new_node is the only parent of _nodes[6]
  EXPECT_EQ(_nodes[6]->parents().size(), 1u);
  ASSERT_LQP_TIE(new_node, LQPChildSide::Left, _nodes[6]);

  // Make sure new_node, _nodes[3] and _nodes[4] are the only parents of _nodes[7]
  EXPECT_EQ(_nodes[7]->parents().size(), 3u);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(new_node, LQPChildSide::Right, _nodes[7]);

  // Make sure _nodes[5] former parents point to new_node.
  ASSERT_LQP_TIE(_nodes[2], LQPChildSide::Left, new_node);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Left, new_node);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Left, new_node);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphReplaceWithLeaf) {
  auto new_node_a = std::make_shared<MockNode>();
  auto new_node_b = std::make_shared<MockNode>();

  _nodes[6]->replace_with(new_node_a);
  _nodes[7]->replace_with(new_node_b);

  // Make sure _nodes[6] is untied from the LQP
  EXPECT_TRUE(_nodes[6]->parents().empty());
  EXPECT_EQ(_nodes[6]->left_child(), nullptr);
  EXPECT_EQ(_nodes[6]->right_child(), nullptr);

  // Make sure _nodes[7] is untied from the LQP
  EXPECT_TRUE(_nodes[7]->parents().empty());
  EXPECT_EQ(_nodes[7]->left_child(), nullptr);
  EXPECT_EQ(_nodes[7]->right_child(), nullptr);

  ASSERT_LQP_TIE(_nodes[5], LQPChildSide::Left, new_node_a);
  ASSERT_LQP_TIE(_nodes[3], LQPChildSide::Right, new_node_b);
  ASSERT_LQP_TIE(_nodes[4], LQPChildSide::Right, new_node_b);
}

}  // namespace opossum
