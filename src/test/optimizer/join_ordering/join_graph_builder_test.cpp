#include "gtest/gtest.h"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/join_edge.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class JoinGraphBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node_a =
        std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x1"}, {DataType::Int, "x2"}});
    _mock_node_b =
        std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "y1"}, {DataType::Int, "y2"}});
    _mock_node_c = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "z1"}});

    _mock_node_a_x1 = _mock_node_a->get_column("x1"s);
    _mock_node_a_x2 = _mock_node_a->get_column("x2"s);
    _mock_node_b_y1 = _mock_node_b->get_column("y1"s);
    _mock_node_b_y2 = _mock_node_b->get_column("y2"s);
    _mock_node_c_z1 = _mock_node_c->get_column("z1"s);
  }

  std::string to_string(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
    std::stringstream stream;
    predicate->print(stream);
    return stream.str();
  }

  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b, _mock_node_c;

  LQPColumnReference _mock_node_a_x1, _mock_node_a_x2, _mock_node_b_y1, _mock_node_b_y2, _mock_node_c_z1;
};

TEST_F(JoinGraphBuilderTest, MultipleComponents) {
  /**
   * Test that the JoinGraphBuilder correctly handles LQPs with Components that are not connected with Predicates.
   * Consider e.g. the simple query `SELECT * FROM A,B,C WHERE A.a = B.b AND C.c > 5`. Here A and B form one component
   * and the single vertex C is another one. They should be connected with an unpredicated (i.e. Cross Join) edge, e.g.,
   * between A and C
   */

  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{_mock_node_a_x2, _mock_node_b_y2}, PredicateCondition::Equals,
    JoinNode::make(JoinMode::Cross,
      _mock_node_a, _mock_node_c),
    _mock_node_b);
  // clang-format on

  auto join_graph = JoinGraphBuilder{}(lqp);  // NOLINT

  /**
   * Test vertices
   */
  ASSERT_EQ(join_graph->vertices.size(), 3u);

  EXPECT_EQ(join_graph->vertices.at(0), _mock_node_a);
  EXPECT_EQ(join_graph->vertices.at(1), _mock_node_c);
  EXPECT_EQ(join_graph->vertices.at(2), _mock_node_b);

  /**
   * Test edges
   */
  const auto edge_a = join_graph->find_edge(JoinVertexSet{3, 0b110});
  ASSERT_NE(edge_a, nullptr);
  ASSERT_EQ(edge_a->predicates.size(), 0u);

  const auto edge_b = join_graph->find_edge(JoinVertexSet{3, 0b101});
  ASSERT_NE(edge_b, nullptr);
  ASSERT_EQ(edge_b->predicates.size(), 1u);
  EXPECT_EQ(to_string(edge_b->predicates.at(0)), "x2 = y2");

  /**
   * Test output relations
   */
  EXPECT_EQ(join_graph->output_relations.size(), 0u);
}

TEST_F(JoinGraphBuilderTest, ComplexLQP) {
  //  [0] [Projection] z1, y1
  //   \_[1] [Predicate] x2 <= z1
  //      \_[2] [Cross Join]
  //         \_[3] [Predicate] sum_a <= y1
  //         |  \_[4] [Predicate] y1 > 32
  //         |     \_[5] [Inner Join] x2 = y2
  //         |        \_[6] [Predicate] sum_a = 5
  //         |        |  \_[7] [Aggregate] SUM(x1) AS "sum_a" GROUP BY [x2]
  //         |        |     \_[8] [MockTable]
  //         |        \_[9] [Predicate] y2 < 200
  //         |           \_[10] [UnionNode] Mode: UnionPositions
  //         |              \_[11] [Predicate] y2 = 7
  //         |              |  \_[12] [Predicate] y1 = 6
  //         |              |     \_[13] [MockTable]
  //         |              \_[14] [Predicate] y1 >= 8
  //         |                 \_Recurring Node --> [13]
  //         \_[15] [MockTable]

  const auto sum_expression = LQPExpression::create_aggregate_function(
      AggregateFunction::Sum, {LQPExpression::create_column(_mock_node_a_x1)}, "sum_a");

  auto aggregate_node_a = std::make_shared<AggregateNode>(std::vector<std::shared_ptr<LQPExpression>>{sum_expression},
                                                          std::vector<LQPColumnReference>{_mock_node_a_x2});

  aggregate_node_a->set_left_input(_mock_node_a);

  auto sum_mock_node_a_x1 = aggregate_node_a->get_column("sum_a"s);

  auto predicate_node_a = std::make_shared<PredicateNode>(sum_mock_node_a_x1, PredicateCondition::Equals, 5);
  auto predicate_node_b = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::Equals, 6);
  auto predicate_node_c = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::Equals, 7);
  auto predicate_node_d = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::GreaterThanEquals, 8);
  auto predicate_node_e = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::LessThan, 200);
  auto predicate_node_f = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::GreaterThan, 32);
  auto predicate_node_g =
      std::make_shared<PredicateNode>(sum_mock_node_a_x1, PredicateCondition::LessThanEquals, _mock_node_b_y1);
  auto predicate_node_h =
      std::make_shared<PredicateNode>(_mock_node_a_x2, PredicateCondition::LessThanEquals, _mock_node_c_z1);
  auto predicate_node_i = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::Equals, 9);
  auto predicate_node_j = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::Between, 1, 42);

  auto inner_join_node_a = std::make_shared<JoinNode>(
      JoinMode::Inner, LQPColumnReferencePair{_mock_node_a_x2, _mock_node_b_y2}, PredicateCondition::Equals);
  auto cross_join_node_a = std::make_shared<JoinNode>(JoinMode::Cross);

  auto union_node_a = std::make_shared<UnionNode>(UnionMode::Positions);
  auto union_node_b = std::make_shared<UnionNode>(UnionMode::Positions);

  auto _projection_node_a = std::make_shared<ProjectionNode>(std::vector<std::shared_ptr<LQPExpression>>{
      LQPExpression::create_column(_mock_node_c_z1), LQPExpression::create_column(_mock_node_b_y1)});

  auto lqp = _projection_node_a;

  /**
   * Wire up LQP
   */
  _projection_node_a->set_left_input(predicate_node_j);
  predicate_node_j->set_left_input(predicate_node_h);
  predicate_node_h->set_left_input(cross_join_node_a);
  cross_join_node_a->set_left_input(predicate_node_g);
  cross_join_node_a->set_right_input(_mock_node_c);
  predicate_node_g->set_left_input(predicate_node_f);
  predicate_node_f->set_left_input(inner_join_node_a);
  inner_join_node_a->set_left_input(predicate_node_a);
  inner_join_node_a->set_right_input(predicate_node_e);
  predicate_node_e->set_left_input(union_node_a);
  union_node_a->set_left_input(predicate_node_c);
  union_node_a->set_right_input(union_node_b);
  predicate_node_c->set_left_input(predicate_node_b);
  predicate_node_b->set_left_input(_mock_node_b);
  union_node_b->set_left_input(predicate_node_d);
  union_node_b->set_right_input(predicate_node_i);
  predicate_node_d->set_left_input(_mock_node_b);
  predicate_node_i->set_left_input(_mock_node_b);
  predicate_node_a->set_left_input(aggregate_node_a);

  auto join_graph = JoinGraphBuilder{}(lqp);  // NOLINT

  /**
   * Test that the expected JoinGraph has been generated
   */

  /**
   * Test vertices
   */
  ASSERT_EQ(join_graph->vertices.size(), 3u);

  EXPECT_EQ(join_graph->vertices.at(0), aggregate_node_a);
  EXPECT_EQ(join_graph->vertices.at(1), _mock_node_b);
  EXPECT_EQ(join_graph->vertices.at(2), _mock_node_c);

  /**
   * Test edges
   */
  const auto edge_a = join_graph->find_edge(JoinVertexSet{3, 0b001});
  ASSERT_NE(edge_a, nullptr);
  ASSERT_EQ(edge_a->predicates.size(), 1u);
  EXPECT_EQ(to_string(edge_a->predicates.at(0)), "sum_a = 5");

  const auto edge_b = join_graph->find_edge(JoinVertexSet{3, 0b010});
  ASSERT_NE(edge_b, nullptr);
  ASSERT_EQ(edge_b->predicates.size(), 5u);
  EXPECT_EQ(to_string(edge_b->predicates.at(0)), "y2 >= 1");
  EXPECT_EQ(to_string(edge_b->predicates.at(1)), "y2 <= 42");
  EXPECT_EQ(to_string(edge_b->predicates.at(2)), "y1 > 32");
  EXPECT_EQ(to_string(edge_b->predicates.at(3)), "y2 < 200");
  EXPECT_EQ(to_string(edge_b->predicates.at(4)), "(y2 = 7 AND y1 = 6) OR (y1 >= 8 OR y2 = 9)");

  const auto edge_ab = join_graph->find_edge(JoinVertexSet{3, 0b011});
  ASSERT_NE(edge_ab, nullptr);
  ASSERT_EQ(edge_ab->predicates.size(), 2u);
  EXPECT_EQ(to_string(edge_ab->predicates.at(0)), "sum_a <= y1");
  EXPECT_EQ(to_string(edge_ab->predicates.at(1)), "x2 = y2");

  const auto edge_ac = join_graph->find_edge(JoinVertexSet{3, 0b101});
  ASSERT_NE(edge_ac, nullptr);
  ASSERT_EQ(edge_ac->predicates.size(), 1u);
  EXPECT_EQ(to_string(edge_ac->predicates.at(0)), "x2 <= z1");

  /**
   * Test output relations
   */
  ASSERT_EQ(join_graph->output_relations.size(), 1u);
  EXPECT_EQ(join_graph->output_relations.at(0).output, _projection_node_a);
  EXPECT_EQ(join_graph->output_relations.at(0).input_side, LQPInputSide::Left);
}

}  // namespace opossum
