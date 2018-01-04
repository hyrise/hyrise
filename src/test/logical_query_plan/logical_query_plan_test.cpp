#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "base_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class LogicalQueryPlanTest : public BaseTest {
 protected:
  void SetUp() override {
    /**
     * Init some nodes for the tests to use
     */
    _mock_node_a =
        std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "t_a");
    _mock_node_b =
        std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "t_b");

    _t_a_a = LQPColumnOrigin{_mock_node_a, ColumnID{0}};
    _t_a_b = LQPColumnOrigin{_mock_node_a, ColumnID{1}};
    _t_b_a = LQPColumnOrigin{_mock_node_b, ColumnID{0}};
    _t_b_b = LQPColumnOrigin{_mock_node_b, ColumnID{1}};

    _predicate_node_a = std::make_shared<PredicateNode>(_t_a_a, ScanType::Equals, 42);
    _predicate_node_b = std::make_shared<PredicateNode>(_t_a_b, ScanType::Equals, 1337);
    _projection_node = std::make_shared<ProjectionNode>(LQPExpression::create_columns({_t_a_a, _t_a_b}));
    _join_node = std::make_shared<JoinNode>(JoinMode::Inner, JoinColumnOrigins{_t_a_a, _t_b_a}, ScanType::Equals);

    /**
     * Init complex graph.
     * #[0] [MockTable]
     *   \_[1] [MockTable]
     *   |  \_[2] [MockTable]
     *   |  |  \_[3] [MockTable]
     *   |  |     \_[4] [MockTable]
     *   |  |     \_[5] [MockTable]
     *   |  \_[6] [MockTable]
     *   |     \_Recurring Node --> [3]
     *   |     \_Recurring Node --> [5]
     *   \_[7] [MockTable]
     *     \_Recurring Node --> [3]
     *     \_Recurring Node --> [5]
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

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<PredicateNode> _predicate_node_a;
  std::shared_ptr<PredicateNode> _predicate_node_b;
  std::shared_ptr<ProjectionNode> _projection_node;
  std::shared_ptr<JoinNode> _join_node;

  LQPColumnOrigin _t_a_a;
  LQPColumnOrigin _t_a_b;
  LQPColumnOrigin _t_b_a;
  LQPColumnOrigin _t_b_b;
};

TEST_F(LogicalQueryPlanTest, SimpleParentTest) {
  ASSERT_EQ(_mock_node_a->left_child(), nullptr);
  ASSERT_EQ(_mock_node_a->right_child(), nullptr);
  ASSERT_TRUE(_mock_node_a->parents().empty());

  _predicate_node_a->set_left_child(_mock_node_a);

  ASSERT_EQ(_mock_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_child(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_child(), nullptr);
  ASSERT_TRUE(_predicate_node_a->parents().empty());

  _projection_node->set_left_child(_predicate_node_a);

  ASSERT_EQ(_predicate_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_projection_node});
  ASSERT_EQ(_projection_node->left_child(), _predicate_node_a);
  ASSERT_EQ(_projection_node->right_child(), nullptr);
  ASSERT_TRUE(_projection_node->parents().empty());

  ASSERT_ANY_THROW(_projection_node->get_child_side(_mock_node_a));
}

TEST_F(LogicalQueryPlanTest, SimpleClearParentsTest) {
  _predicate_node_a->set_left_child(_mock_node_a);

  ASSERT_EQ(_mock_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_child(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_child(), nullptr);
  ASSERT_TRUE(_predicate_node_a->parents().empty());

  _mock_node_a->clear_parents();

  ASSERT_TRUE(_mock_node_a->parents().empty());
  ASSERT_EQ(_predicate_node_a->left_child(), nullptr);
  ASSERT_EQ(_predicate_node_a->right_child(), nullptr);
  ASSERT_TRUE(_predicate_node_a->parents().empty());
}

TEST_F(LogicalQueryPlanTest, ChainSameNodesTest) {
  ASSERT_EQ(_mock_node_a->left_child(), nullptr);
  ASSERT_EQ(_mock_node_a->right_child(), nullptr);
  ASSERT_TRUE(_mock_node_a->parents().empty());

  _predicate_node_a->set_left_child(_mock_node_a);

  ASSERT_EQ(_mock_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_child(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_child(), nullptr);
  ASSERT_TRUE(_predicate_node_a->parents().empty());

  _predicate_node_b->set_left_child(_predicate_node_a);

  ASSERT_EQ(_predicate_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_b});
  ASSERT_EQ(_predicate_node_b->left_child(), _predicate_node_a);
  ASSERT_EQ(_predicate_node_b->right_child(), nullptr);
  ASSERT_TRUE(_predicate_node_b->parents().empty());

  _projection_node->set_left_child(_predicate_node_b);

  ASSERT_EQ(_predicate_node_b->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_projection_node});
  ASSERT_EQ(_projection_node->left_child(), _predicate_node_b);
  ASSERT_EQ(_projection_node->right_child(), nullptr);
  ASSERT_TRUE(_projection_node->parents().empty());
}

TEST_F(LogicalQueryPlanTest, TwoInputsTest) {
  ASSERT_EQ(_join_node->left_child(), nullptr);
  ASSERT_EQ(_join_node->right_child(), nullptr);
  ASSERT_TRUE(_join_node->parents().empty());

  _join_node->set_left_child(_mock_node_a);
  _join_node->set_right_child(_mock_node_b);

  ASSERT_EQ(_join_node->left_child(), _mock_node_a);
  ASSERT_EQ(_join_node->right_child(), _mock_node_b);
  ASSERT_TRUE(_join_node->parents().empty());

  ASSERT_EQ(_mock_node_a->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_join_node});
  ASSERT_EQ(_mock_node_b->parents(), std::vector<std::shared_ptr<AbstractLQPNode>>{_join_node});
}

TEST_F(LogicalQueryPlanTest, AliasedSubqueryTest) {
  _predicate_node_a->set_left_child(_mock_node_a);

  ASSERT_EQ(_predicate_node_a->find_table_name_origin("t_a"), _mock_node_a);

  _predicate_node_a->set_alias(std::string("foo"));

  ASSERT_EQ(_predicate_node_a->find_table_name_origin("foo"), _predicate_node_a);
  ASSERT_EQ(_predicate_node_a->find_table_name_origin("t_a"), nullptr);

  ASSERT_EQ(_predicate_node_a->get_column_origin_by_named_column_reference({"b"}), _t_a_b);
  ASSERT_EQ(_predicate_node_a->get_column_origin_by_named_column_reference({"b", {"foo"}}), _t_a_b);
  ASSERT_EQ(_predicate_node_a->find_column_origin_by_named_column_reference({"b", "t_a"}), std::nullopt);
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

//  TEST_F(LogicalQueryPlanTest, DeepCopyColumnOrigins) {
//  /**
//   * Test that when calling AbstractLQPNode::deep_copy(), all ColumnOrigins in the new LQP actually reference the
//   Nodes
//   * in that LQP
//   */
//
//  auto mock_node_a = std::make_shared<MockNode>({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});
//  auto mock_node_b = std::make_shared<MockNode>({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});
//  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, JoinColumnOrigins{{mock_node_a, ColumnID{2},
//  {mock_node_b, ColumnID{1}}}}, ScanType::Equals);
//  auto aggregate_node = std::make_shared<AggregateNode>(JoinMode::Inner, JoinColumnOrigins{{mock_node_a, ColumnID{2},
//  {mock_node_b, ColumnID{1}}}}, ScanType::Equals);
//  }

}  // namespace opossum
