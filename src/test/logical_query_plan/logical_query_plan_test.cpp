#include <regex>

#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LogicalQueryPlanTest : public ::testing::Test {
 public:
  void SetUp() override {
    StorageManager::get().add_table("int_int", load_table("src/test/tables/int_int.tbl"));
    StorageManager::get().add_table("int_int_int", load_table("src/test/tables/int_int_int.tbl"));

    node_int_int = StoredTableNode::make("int_int");
    a1 = node_int_int->get_column("a");
    b1 = node_int_int->get_column("b");

    node_int_int_int = StoredTableNode::make("int_int_int");
    a2 = node_int_int_int->get_column("a");
    b2 = node_int_int_int->get_column("b");
    c2 = node_int_int_int->get_column("c");

    /**
     * Init some nodes for the tests to use
     */
    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "t_a");
    _mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "t_b");

    _t_a_a = LQPColumnReference{_mock_node_a, ColumnID{0}};
    _t_a_b = LQPColumnReference{_mock_node_a, ColumnID{1}};
    _t_b_a = LQPColumnReference{_mock_node_b, ColumnID{0}};
    _t_b_b = LQPColumnReference{_mock_node_b, ColumnID{1}};

    _predicate_node_a = PredicateNode::make(equals_(_t_a_a, 42));
    _predicate_node_b = PredicateNode::make(equals_(_t_a_b, 1337));
    _projection_node = ProjectionNode::make(expression_vector(_t_a_a, _t_a_b));
    _join_node = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_a));

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
    _nodes[6] = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}}});
    _nodes[7] = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "b"}}});
    _nodes[0] = JoinNode::make(JoinMode::Cross);
    _nodes[1] = JoinNode::make(JoinMode::Cross);
    _nodes[2] = PredicateNode::make(equals_(LQPColumnReference{_nodes[6], ColumnID{0}}, 42));
    _nodes[3] = JoinNode::make(JoinMode::Cross);
    _nodes[4] = JoinNode::make(JoinMode::Cross);
    _nodes[5] = JoinNode::make(JoinMode::Cross);

    _nodes[5]->set_right_input(_nodes[7]);
    _nodes[0]->set_right_input(_nodes[4]);
    _nodes[4]->set_left_input(_nodes[5]);
    _nodes[4]->set_right_input(_nodes[7]);
    _nodes[5]->set_left_input(_nodes[6]);
    _nodes[2]->set_left_input(_nodes[5]);
    _nodes[1]->set_right_input(_nodes[3]);
    _nodes[3]->set_left_input(_nodes[5]);
    _nodes[3]->set_right_input(_nodes[7]);
    _nodes[1]->set_left_input(_nodes[2]);
    _nodes[0]->set_left_input(_nodes[1]);
  }

  void TearDown() override { StorageManager::reset(); }

  std::shared_ptr<Table> table_int_int;
  std::shared_ptr<StoredTableNode> node_int_int, node_int_int_int;
  LQPColumnReference a1, b1;
  LQPColumnReference a2, b2, c2;

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

TEST_F(LogicalQueryPlanTest, SimpleOutputTest) {
  ASSERT_EQ(_mock_node_a->left_input(), nullptr);
  ASSERT_EQ(_mock_node_a->right_input(), nullptr);
  ASSERT_TRUE(_mock_node_a->outputs().empty());

  _predicate_node_a->set_left_input(_mock_node_a);

  ASSERT_EQ(_mock_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_input(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_input(), nullptr);
  ASSERT_TRUE(_predicate_node_a->outputs().empty());

  _projection_node->set_left_input(_predicate_node_a);

  ASSERT_EQ(_predicate_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_projection_node});
  ASSERT_EQ(_projection_node->left_input(), _predicate_node_a);
  ASSERT_EQ(_projection_node->right_input(), nullptr);
  ASSERT_TRUE(_projection_node->outputs().empty());

  ASSERT_ANY_THROW(_projection_node->get_input_side(_mock_node_a));
}

TEST_F(LogicalQueryPlanTest, SimpleClearOutputs) {
  _predicate_node_a->set_left_input(_mock_node_a);

  ASSERT_EQ(_mock_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_input(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_input(), nullptr);
  ASSERT_TRUE(_predicate_node_a->outputs().empty());

  _mock_node_a->clear_outputs();

  ASSERT_TRUE(_mock_node_a->outputs().empty());
  ASSERT_EQ(_predicate_node_a->left_input(), nullptr);
  ASSERT_EQ(_predicate_node_a->right_input(), nullptr);
  ASSERT_TRUE(_predicate_node_a->outputs().empty());
}

TEST_F(LogicalQueryPlanTest, ChainSameNodesTest) {
  ASSERT_EQ(_mock_node_a->left_input(), nullptr);
  ASSERT_EQ(_mock_node_a->right_input(), nullptr);
  ASSERT_TRUE(_mock_node_a->outputs().empty());

  _predicate_node_a->set_left_input(_mock_node_a);

  ASSERT_EQ(_mock_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_a});
  ASSERT_EQ(_predicate_node_a->left_input(), _mock_node_a);
  ASSERT_EQ(_predicate_node_a->right_input(), nullptr);
  ASSERT_TRUE(_predicate_node_a->outputs().empty());

  _predicate_node_b->set_left_input(_predicate_node_a);

  ASSERT_EQ(_predicate_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_predicate_node_b});
  ASSERT_EQ(_predicate_node_b->left_input(), _predicate_node_a);
  ASSERT_EQ(_predicate_node_b->right_input(), nullptr);
  ASSERT_TRUE(_predicate_node_b->outputs().empty());

  _projection_node->set_left_input(_predicate_node_b);

  ASSERT_EQ(_predicate_node_b->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_projection_node});
  ASSERT_EQ(_projection_node->left_input(), _predicate_node_b);
  ASSERT_EQ(_projection_node->right_input(), nullptr);
  ASSERT_TRUE(_projection_node->outputs().empty());
}

TEST_F(LogicalQueryPlanTest, TwoInputsTest) {
  ASSERT_EQ(_join_node->left_input(), nullptr);
  ASSERT_EQ(_join_node->right_input(), nullptr);
  ASSERT_TRUE(_join_node->outputs().empty());

  _join_node->set_left_input(_mock_node_a);
  _join_node->set_right_input(_mock_node_b);

  ASSERT_EQ(_join_node->left_input(), _mock_node_a);
  ASSERT_EQ(_join_node->right_input(), _mock_node_b);
  ASSERT_TRUE(_join_node->outputs().empty());

  ASSERT_EQ(_mock_node_a->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_join_node});
  ASSERT_EQ(_mock_node_b->outputs(), std::vector<std::shared_ptr<AbstractLQPNode>>{_join_node});
}

TEST_F(LogicalQueryPlanTest, ComplexGraphStructure) {
  ASSERT_LQP_TIE(_nodes[0], LQPInputSide::Left, _nodes[1]);
  ASSERT_LQP_TIE(_nodes[0], LQPInputSide::Right, _nodes[4]);
  ASSERT_LQP_TIE(_nodes[1], LQPInputSide::Left, _nodes[2]);
  ASSERT_LQP_TIE(_nodes[1], LQPInputSide::Right, _nodes[3]);
  ASSERT_LQP_TIE(_nodes[2], LQPInputSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[5], LQPInputSide::Left, _nodes[6]);
  ASSERT_LQP_TIE(_nodes[5], LQPInputSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Right, _nodes[7]);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphRemoveFromTree) {
  lqp_remove_node(_nodes[2]);

  EXPECT_TRUE(_nodes[2]->outputs().empty());
  EXPECT_EQ(_nodes[2]->left_input(), nullptr);
  EXPECT_EQ(_nodes[2]->right_input(), nullptr);

  // Make sure _node[1], _node[3] and _node[4] are the only outputs _nodes[5] has
  EXPECT_EQ(_nodes[5]->outputs().size(), 3u);
  ASSERT_LQP_TIE(_nodes[1], LQPInputSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Left, _nodes[5]);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Left, _nodes[5]);

  ASSERT_LQP_TIE(_nodes[1], LQPInputSide::Right, _nodes[3]);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphRemoveFromTreeLeaf) {
  lqp_remove_node(_nodes[6]);
  lqp_remove_node(_nodes[7]);

  EXPECT_TRUE(_nodes[6]->outputs().empty());
  EXPECT_TRUE(_nodes[7]->outputs().empty());
  EXPECT_EQ(_nodes[6]->left_input(), nullptr);
  EXPECT_EQ(_nodes[6]->right_input(), nullptr);
  EXPECT_EQ(_nodes[7]->left_input(), nullptr);
  EXPECT_EQ(_nodes[7]->right_input(), nullptr);
  EXPECT_EQ(_nodes[5]->left_input(), nullptr);
  EXPECT_EQ(_nodes[3]->right_input(), nullptr);
  EXPECT_EQ(_nodes[4]->right_input(), nullptr);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphReplaceWith) {
  auto new_node = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});

  lqp_replace_node(_nodes[5], new_node);

  // Make sure _nodes[5] is untied from the LQP
  EXPECT_TRUE(_nodes[5]->outputs().empty());
  EXPECT_EQ(_nodes[5]->left_input(), nullptr);
  EXPECT_EQ(_nodes[5]->right_input(), nullptr);

  // Make sure new_node is the only output of _nodes[6]
  EXPECT_EQ(_nodes[6]->outputs().size(), 1u);
  ASSERT_LQP_TIE(new_node, LQPInputSide::Left, _nodes[6]);

  // Make sure new_node, _nodes[3] and _nodes[4] are the only outputs of _nodes[7]
  EXPECT_EQ(_nodes[7]->outputs().size(), 3u);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Right, _nodes[7]);
  ASSERT_LQP_TIE(new_node, LQPInputSide::Right, _nodes[7]);

  // Make sure _nodes[5] former outputs point to new_node.
  ASSERT_LQP_TIE(_nodes[2], LQPInputSide::Left, new_node);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Left, new_node);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Left, new_node);
}

TEST_F(LogicalQueryPlanTest, ComplexGraphReplaceWithLeaf) {
  auto new_node_a = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  auto new_node_b = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});

  lqp_replace_node(_nodes[6], new_node_a);
  lqp_replace_node(_nodes[7], new_node_b);

  // Make sure _nodes[6] is untied from the LQP
  EXPECT_TRUE(_nodes[6]->outputs().empty());
  EXPECT_EQ(_nodes[6]->left_input(), nullptr);
  EXPECT_EQ(_nodes[6]->right_input(), nullptr);

  // Make sure _nodes[7] is untied from the LQP
  EXPECT_TRUE(_nodes[7]->outputs().empty());
  EXPECT_EQ(_nodes[7]->left_input(), nullptr);
  EXPECT_EQ(_nodes[7]->right_input(), nullptr);

  ASSERT_LQP_TIE(_nodes[5], LQPInputSide::Left, new_node_a);
  ASSERT_LQP_TIE(_nodes[3], LQPInputSide::Right, new_node_b);
  ASSERT_LQP_TIE(_nodes[4], LQPInputSide::Right, new_node_b);
}

TEST_F(LogicalQueryPlanTest, CreateNodeMapping) {
  const auto projection_node = ProjectionNode::make(node_int_int->column_expressions(), node_int_int);
  const auto lqp = projection_node;

  const auto copied_lqp = lqp->deep_copy();
  const auto copied_projection_node = std::dynamic_pointer_cast<ProjectionNode>(copied_lqp);
  const auto copied_node_int_int = std::dynamic_pointer_cast<StoredTableNode>(copied_lqp->left_input());

  auto node_mapping = lqp_create_node_mapping(lqp, copied_lqp);

  EXPECT_EQ(node_mapping[projection_node], copied_projection_node);
  EXPECT_EQ(node_mapping[node_int_int], copied_node_int_int);
}

TEST_F(LogicalQueryPlanTest, DeepCopyBasics) {
  const auto expression_a = std::make_shared<LQPColumnExpression>(LQPColumnReference{node_int_int, ColumnID{0}});
  const auto expression_b = std::make_shared<LQPColumnExpression>(LQPColumnReference{node_int_int, ColumnID{1}});

  const auto projection_node = ProjectionNode::make(node_int_int->column_expressions(), node_int_int);
  const auto lqp = projection_node;

  const auto copied_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(copied_lqp, lqp);

  const auto copied_projection_node = std::dynamic_pointer_cast<ProjectionNode>(copied_lqp);
  const auto copied_node_int_int = std::dynamic_pointer_cast<StoredTableNode>(copied_lqp->left_input());

  // Nodes in copied LQP should have different pointers
  EXPECT_NE(projection_node, copied_projection_node);
  EXPECT_NE(node_int_int, copied_node_int_int);

  // Check that expressions in copied LQP point to StoredTableNode in their LQP, not into the original LQP
  const auto copied_expression_a =
      std::dynamic_pointer_cast<LQPColumnExpression>(copied_projection_node->expressions.at(0));
  const auto copied_expression_b =
      std::dynamic_pointer_cast<LQPColumnExpression>(copied_projection_node->expressions.at(1));

  EXPECT_EQ(copied_expression_a->column_reference.original_node(), copied_node_int_int);
  EXPECT_EQ(copied_expression_b->column_reference.original_node(), copied_node_int_int);
}

TEST_F(LogicalQueryPlanTest, PrintWithoutSubselects) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a1, 5),
    JoinNode::make(JoinMode::Inner, equals_(a1, a2),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(equals_(a1, 5), node_int_int),
        PredicateNode::make(equals_(a1, 6), node_int_int)),
    node_int_int_int));
  // clang-format on

  std::stringstream stream;
  lqp->print(stream);

  EXPECT_EQ(stream.str(), R"([0] [Predicate] a > 5
 \_[1] [Join] Mode: Inner a = a
    \_[2] [UnionNode] Mode: UnionPositions
    |  \_[3] [Predicate] a = 5
    |  |  \_[4] [StoredTable] Name: 'int_int'
    |  \_[5] [Predicate] a = 6
    |     \_Recurring Node --> [4]
    \_[6] [StoredTable] Name: 'int_int_int'
)");
}

TEST_F(LogicalQueryPlanTest, PrintWithSubselects) {
  // clang-format off
  const auto subselect_b_lqp =
  PredicateNode::make(equals_(a2, 5), node_int_int_int);
  const auto subselect_b = lqp_select_(subselect_b_lqp);

  const auto subselect_a_lqp =
  PredicateNode::make(equals_(a2, subselect_b), node_int_int_int);
  const auto subselect_a = lqp_select_(subselect_a_lqp);

  const auto lqp =
  PredicateNode::make(greater_than_(a1, subselect_a), node_int_int);
  // clang-format on

  std::stringstream stream;
  lqp->print(stream);

  // Result is undeterministic, but should look something like (order and addresses may vary)
  // [0] [Predicate] a > SUBSELECT (LQP, 0x4e2bda0, Parameters: )
  //  \_[1] [StoredTable] Name: 'int_int'
  // -------- Subselects ---------
  // 0x4e2d160:
  // [0] [Predicate] a = 5
  //  \_[1] [StoredTable] Name: 'int_int_int'

  // 0x4e2bda0:
  // [0] [Predicate] a = SUBSELECT (LQP, 0x4e2d160, Parameters: )
  //  \_[1] [StoredTable] Name: 'int_int_int'

  EXPECT_TRUE(std::regex_search(stream.str().c_str(),
                                std::regex{R"(\[0\] \[Predicate\] a \> SUBSELECT \(LQP, 0x[a-z0-9]+\))"}));
  EXPECT_TRUE(std::regex_search(stream.str().c_str(), std::regex{"Subselects"}));
  EXPECT_TRUE(
      std::regex_search(stream.str().c_str(), std::regex{R"(\[0\] \[Predicate\] a = SUBSELECT \(LQP, 0x[a-z0-9]+\))"}));
  EXPECT_TRUE(std::regex_search(stream.str().c_str(), std::regex{R"(\[0\] \[Predicate\] a = 5)"}));
}

TEST_F(LogicalQueryPlanTest, DeepCopySubSelects) {
  const auto parameter_a = correlated_parameter_(ParameterID{0}, b1);

  // clang-format off
  const auto sub_select_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(add_(a2, parameter_a))),
    ProjectionNode::make(expression_vector(a2, b2, add_(a2, parameter_a)),
      node_int_int_int));
  const auto sub_select = lqp_select_(sub_select_lqp, std::make_pair(ParameterID{0}, b1));

  const auto lqp =
  ProjectionNode::make(expression_vector(a1, sub_select),
    PredicateNode::make(greater_than_(a1, sub_select),
      ProjectionNode::make(expression_vector(sub_select, a1, b1),
        node_int_int)));
  // clang-format on

  const auto copied_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(copied_lqp, lqp);
  const auto copied_projection_a = std::dynamic_pointer_cast<ProjectionNode>(copied_lqp);
  const auto copied_predicate_a = std::dynamic_pointer_cast<PredicateNode>(copied_lqp->left_input());

  const auto copied_sub_select_a =
      std::dynamic_pointer_cast<LQPSelectExpression>(copied_lqp->column_expressions().at(1));
  const auto copied_sub_select_b =
      std::dynamic_pointer_cast<LQPSelectExpression>(copied_predicate_a->predicate->arguments.at(1));

  // Check that LQPs and SelectExpressions were actually duplicated
  EXPECT_NE(copied_sub_select_a, sub_select);
  EXPECT_NE(copied_sub_select_a->lqp, sub_select->lqp);
  EXPECT_NE(copied_sub_select_b, sub_select);
  EXPECT_NE(copied_sub_select_b->lqp, sub_select->lqp);
}

TEST_F(LogicalQueryPlanTest, OutputResetOnNodeDelete) {
  auto mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}});
  auto mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "y"}});
  auto join_node = JoinNode::make(JoinMode::Cross, mock_node_a, mock_node_b);

  EXPECT_EQ(mock_node_a->output_count(), 1u);
  EXPECT_EQ(mock_node_b->output_count(), 1u);

  join_node.reset();

  EXPECT_EQ(mock_node_a->output_count(), 0u);
  EXPECT_EQ(mock_node_b->output_count(), 0u);
}

}  // namespace opossum
