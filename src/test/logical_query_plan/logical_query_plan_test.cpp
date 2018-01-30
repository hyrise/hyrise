#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
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

    _t_a_a = LQPColumnReference{_mock_node_a, ColumnID{0}};
    _t_a_b = LQPColumnReference{_mock_node_a, ColumnID{1}};
    _t_b_a = LQPColumnReference{_mock_node_b, ColumnID{0}};
    _t_b_b = LQPColumnReference{_mock_node_b, ColumnID{1}};

    _predicate_node_a = std::make_shared<PredicateNode>(_t_a_a, PredicateCondition::Equals, 42);
    _predicate_node_b = std::make_shared<PredicateNode>(_t_a_b, PredicateCondition::Equals, 1337);
    _projection_node = std::make_shared<ProjectionNode>(LQPExpression::create_columns({_t_a_a, _t_a_b}));
    _join_node =
        std::make_shared<JoinNode>(JoinMode::Inner, LQPColumnReferencePair{_t_a_a, _t_b_a}, PredicateCondition::Equals);

    /**
     * Init complex graph.
     * [0] [Cross Join]
     *  \_[1] [Cross Join]
     *  |  \_[2] [Predicate] a = 42
     *  |  |  \_[3] [Cross Join]
     *  |  |     \_[4] [MockTable]
     *  |  |     \_[5] [MockTable]
     *  |  \_[6] [Cross Join]
     *  |     \_Recurring Node --> [3]
     *  |     \_Recurring Node --> [5]
     *  \_[7] [Cross Join]
     *     \_Recurring Node --> [3]
     *     \_Recurring Node --> [5]
     */
    _nodes[6] = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{{DataType::Int, "a"}}});
    _nodes[7] = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{{DataType::Int, "b"}}});
    _nodes[0] = std::make_shared<JoinNode>(JoinMode::Cross);
    _nodes[1] = std::make_shared<JoinNode>(JoinMode::Cross);
    _nodes[2] =
        std::make_shared<PredicateNode>(LQPColumnReference{_nodes[6], ColumnID{0}}, PredicateCondition::Equals, 42);
    _nodes[3] = std::make_shared<JoinNode>(JoinMode::Cross);
    _nodes[4] = std::make_shared<JoinNode>(JoinMode::Cross);
    _nodes[5] = std::make_shared<JoinNode>(JoinMode::Cross);

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

  std::array<std::shared_ptr<AbstractLQPNode>, 8> _nodes;

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<PredicateNode> _predicate_node_a;
  std::shared_ptr<PredicateNode> _predicate_node_b;
  std::shared_ptr<ProjectionNode> _projection_node;
  std::shared_ptr<JoinNode> _join_node;

  LQPColumnReference _t_a_a;
  LQPColumnReference _t_a_b;
  LQPColumnReference _t_b_a;
  LQPColumnReference _t_b_b;
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

  ASSERT_EQ(_predicate_node_a->get_column({"b"}), _t_a_b);
  ASSERT_EQ(_predicate_node_a->get_column({"b", {"foo"}}), _t_a_b);
  ASSERT_EQ(_predicate_node_a->find_column({"b", "t_a"}), std::nullopt);
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

  ASSERT_EQ(stream.str(), R"([0] [Cross Join]
 \_[1] [Cross Join]
 |  \_[2] [Predicate] a = 42
 |  |  \_[3] [Cross Join]
 |  |     \_[4] [MockTable]
 |  |     \_[5] [MockTable]
 |  \_[6] [Cross Join]
 |     \_Recurring Node --> [3]
 |     \_Recurring Node --> [5]
 \_[7] [Cross Join]
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
  auto new_node = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});

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
  auto new_node_a = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  auto new_node_b = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});

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

TEST_F(LogicalQueryPlanTest, ColumnReferenceCloning) {
  /**
   * Test AbstractLQPNode::deep_copy_column_reference()
   */

  auto mock_node_a = std::make_shared<MockNode>(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});
  auto mock_node_b =
      std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});
  auto join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  auto predicate_node =
      std::make_shared<PredicateNode>(LQPColumnReference{mock_node_b, ColumnID{0}}, PredicateCondition::Equals, 3);

  const auto column_reference_a = LQPColumnReference{mock_node_a, ColumnID{1}};
  const auto column_reference_b = LQPColumnReference{mock_node_b, ColumnID{0}};

  auto aggregate_node = std::make_shared<AggregateNode>(
      std::vector<std::shared_ptr<LQPExpression>>({LQPExpression::create_aggregate_function(
          AggregateFunction::Sum, {LQPExpression::create_column(column_reference_a)})}),
      std::vector<LQPColumnReference>{{column_reference_b}});

  aggregate_node->set_left_child(predicate_node);
  predicate_node->set_left_child(join_node);
  join_node->set_left_child(mock_node_a);
  join_node->set_right_child(mock_node_b);

  const auto lqp = aggregate_node;
  const auto lqp_copy = lqp->deep_copy();

  const auto column_reference_c = LQPColumnReference{aggregate_node, ColumnID{1}};

  /**
   * Test that column_reference_a and column_reference_b can be resolved from the JoinNode
   */
  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_a, lqp->left_child(),
                                                                     lqp_copy->left_child())
                .original_column_id(),
            column_reference_a.original_column_id());
  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_a, lqp->left_child(),
                                                                     lqp_copy->left_child())
                .original_node(),
            lqp_copy->left_child()->left_child()->left_child());

  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_b, lqp->left_child(),
                                                                     lqp_copy->left_child())
                .original_column_id(),
            column_reference_b.original_column_id());
  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_b, lqp->left_child(),
                                                                     lqp_copy->left_child())
                .original_node(),
            lqp_copy->left_child()->left_child()->right_child());

  /**
   * column_reference_b can be resolved from the Aggregate since it is a GroupByColumn
   */
  EXPECT_EQ(
      AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_b, lqp, lqp_copy).original_column_id(),
      column_reference_b.original_column_id());
  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_b, lqp, lqp_copy).original_node(),
            lqp_copy->left_child()->left_child()->right_child());

  /**
   * SUM(a) can be resolved from the Aggregate
   */
  EXPECT_EQ(
      AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_c, lqp, lqp_copy).original_column_id(),
      column_reference_c.original_column_id());
  EXPECT_EQ(AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_c, lqp, lqp_copy).original_node(),
            lqp_copy);
}

TEST_F(LogicalQueryPlanTest, ColumnIDByColumnReference) {
  /**
   * Test AbstractLQPNode::{get, find}_output_column_id_by_column_reference
   */

  auto mock_node_a = std::make_shared<MockNode>(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});
  auto mock_node_b =
      std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});
  const auto column_reference_a = LQPColumnReference{mock_node_a, ColumnID{0}};
  const auto column_reference_b = LQPColumnReference{mock_node_a, ColumnID{1}};
  auto aggregate_node = std::make_shared<AggregateNode>(
      std::vector<std::shared_ptr<LQPExpression>>({LQPExpression::create_aggregate_function(
          AggregateFunction::Sum, {LQPExpression::create_column(column_reference_a)})}),
      std::vector<LQPColumnReference>{{column_reference_b}});

  aggregate_node->set_left_child(mock_node_a);

  const auto column_reference_c = LQPColumnReference{aggregate_node, ColumnID{1}};

  EXPECT_EQ(mock_node_a->get_output_column_id(column_reference_a), ColumnID{0});
  EXPECT_EQ(mock_node_a->get_output_column_id(column_reference_b), ColumnID{1});
  EXPECT_EQ(aggregate_node->get_output_column_id(column_reference_b), ColumnID{0});
  EXPECT_EQ(aggregate_node->get_output_column_id(column_reference_c), ColumnID{1});
  EXPECT_EQ(aggregate_node->find_output_column_id(column_reference_a), std::nullopt);
  EXPECT_EQ(mock_node_a->find_output_column_id(LQPColumnReference{mock_node_b, ColumnID{0}}), std::nullopt);
}

}  // namespace opossum
