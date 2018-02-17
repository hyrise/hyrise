#include "gtest/gtest.h"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/join_edge.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/join_graph_converter.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class JoinGraphConverterTest: public ::testing::Test {
 protected:
  void SetUp() override {
    //[0] [Projection] z1, y1
    // \_[1] [Predicate] x2 <= z1
    //    \_[2] [Cross Join]
    //       \_[3] [Predicate] sum_a <= y1
    //       |  \_[4] [Predicate] y1 > 32
    //       |     \_[5] [Inner Join] x2 = y2
    //       |        \_[6] [Predicate] sum_a = 5
    //       |        |  \_[7] [Aggregate] SUM(x1) AS "sum_a" GROUP BY [x2]
    //       |        |     \_[8] [MockTable]
    //       |        \_[9] [Predicate] y2 < 200
    //       |           \_[10] [UnionNode] Mode: UnionPositions
    //       |              \_[11] [Predicate] y2 = 7
    //       |              |  \_[12] [Predicate] y1 = 6
    //       |              |     \_[13] [MockTable]
    //       |              \_[14] [Predicate] y1 >= 8
    //       |                 \_Recurring Node --> [13]
    //       \_[15] [MockTable]

    _mock_node_a = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x1"}, {DataType::Int, "x2"}});
    _mock_node_b = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "y1"}, {DataType::Int, "y2"}});
    _mock_node_c = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "z1"}});

    _mock_node_a_x1 = _mock_node_a->get_column("x1"s);
    _mock_node_a_x2 = _mock_node_a->get_column("x2"s);
    _mock_node_b_y1 = _mock_node_b->get_column("y1"s);
    _mock_node_b_y2 = _mock_node_b->get_column("y2"s);
    _mock_node_c_z1 = _mock_node_c->get_column("z1"s);

    const auto sum_expression = LQPExpression::create_aggregate_function(AggregateFunction::Sum, {LQPExpression::create_column(_mock_node_a_x1)}, "sum_a");

    _aggregate_node_a = std::make_shared<AggregateNode>(std::vector<std::shared_ptr<LQPExpression>>{sum_expression}, std::vector<LQPColumnReference>{_mock_node_a_x2});

    _aggregate_node_a->set_left_child(_mock_node_a);

    _sum_mock_node_a_x1 = _aggregate_node_a->get_column("sum_a"s);

    _predicate_node_a = std::make_shared<PredicateNode>(_sum_mock_node_a_x1, PredicateCondition::Equals, 5);
    _predicate_node_b = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::Equals, 6);
    _predicate_node_c = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::Equals, 7);
    _predicate_node_d = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::GreaterThanEquals, 8);
    _predicate_node_e = std::make_shared<PredicateNode>(_mock_node_b_y2, PredicateCondition::LessThan, 200);
    _predicate_node_f = std::make_shared<PredicateNode>(_mock_node_b_y1, PredicateCondition::GreaterThan, 32);
    _predicate_node_g = std::make_shared<PredicateNode>(_sum_mock_node_a_x1, PredicateCondition::LessThanEquals, _mock_node_b_y1);
    _predicate_node_h = std::make_shared<PredicateNode>(_mock_node_a_x2, PredicateCondition::LessThanEquals, _mock_node_c_z1);

    _inner_join_node_a = std::make_shared<JoinNode>(JoinMode::Inner, LQPColumnReferencePair{_mock_node_a_x2, _mock_node_b_y2}, PredicateCondition::Equals);
    _cross_join_node_a = std::make_shared<JoinNode>(JoinMode::Cross);

    _union_node_a = std::make_shared<UnionNode>(UnionMode::Positions);

    _projection_node_a = std::make_shared<ProjectionNode>(std::vector<std::shared_ptr<LQPExpression>>{LQPExpression::create_column(_mock_node_c_z1), LQPExpression::create_column(_mock_node_b_y1)});

    _lqp = _projection_node_a;

    /**
     * Wire up LQP
     */
    _projection_node_a->set_left_child(_predicate_node_h);
    _predicate_node_h->set_left_child(_cross_join_node_a);
    _cross_join_node_a->set_left_child(_predicate_node_g);
    _cross_join_node_a->set_right_child(_mock_node_c);
    _predicate_node_g->set_left_child(_predicate_node_f);
    _predicate_node_f->set_left_child(_inner_join_node_a);
    _inner_join_node_a->set_left_child(_predicate_node_a);
    _inner_join_node_a->set_right_child(_predicate_node_e);
    _predicate_node_e->set_left_child(_union_node_a);
    _union_node_a->set_left_child(_predicate_node_c);
    _union_node_a->set_right_child(_predicate_node_d);
    _predicate_node_c->set_left_child(_predicate_node_b);
    _predicate_node_b->set_left_child(_mock_node_b);
    _predicate_node_d->set_left_child(_mock_node_b);
    _predicate_node_a->set_left_child(_aggregate_node_a);

    _lqp->print();

    _join_graph = JoinGraphConverter{}(_lqp);
  }

  std::string to_string(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
    std::stringstream stream;
    predicate->print(stream);
    return stream.str();
  }

  std::shared_ptr<AggregateNode> _aggregate_node_a;
  std::shared_ptr<JoinNode> _join_node_a, _join_node_b;
  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b, _mock_node_c;
  std::shared_ptr<PredicateNode> _predicate_node_a, _predicate_node_b, _predicate_node_c, _predicate_node_d;
  std::shared_ptr<PredicateNode> _predicate_node_e, _predicate_node_f, _predicate_node_g, _predicate_node_h;
  std::shared_ptr<UnionNode> _union_node_a;
  std::shared_ptr<JoinNode> _inner_join_node_a;
  std::shared_ptr<JoinNode> _cross_join_node_a;
  std::shared_ptr<ProjectionNode> _projection_node_a;
  std::shared_ptr<AbstractLQPNode> _lqp;

  LQPColumnReference _mock_node_a_x1, _mock_node_a_x2, _sum_mock_node_a_x1, _mock_node_b_y1, _mock_node_b_y2, _mock_node_c_z1;

  JoinGraph _join_graph;
};

TEST_F(JoinGraphConverterTest, ComplexLQP) {
  /**
   * Test that the expected JoinGraph has been generated
   */

  /**
   * Test vertices
   */
  ASSERT_EQ(_join_graph.vertices.size(), 3u);

  EXPECT_EQ(_join_graph.vertices.at(0), _aggregate_node_a);
  EXPECT_EQ(_join_graph.vertices.at(1), _mock_node_b);
  EXPECT_EQ(_join_graph.vertices.at(2), _mock_node_c);

  /**
   * Test edges
   */
  const auto edge_a = _join_graph.find_edge(boost::dynamic_bitset<>{3, 0b001});
  ASSERT_NE(edge_a, nullptr);
  ASSERT_EQ(edge_a->predicates.size(), 1u);
  EXPECT_EQ(to_string(edge_a->predicates.at(0)), "sum_a = 5");

  const auto edge_b = _join_graph.find_edge(boost::dynamic_bitset<>{3, 0b010});
  ASSERT_NE(edge_b, nullptr);
  ASSERT_EQ(edge_b->predicates.size(), 3u);
  EXPECT_EQ(to_string(edge_b->predicates.at(0)), "y1 > 32");
  EXPECT_EQ(to_string(edge_b->predicates.at(1)), "y2 < 200");
  EXPECT_EQ(to_string(edge_b->predicates.at(2)), "(y2 = 7 AND y1 = 6) OR y1 >= 8");

  const auto edge_ab = _join_graph.find_edge(boost::dynamic_bitset<>{3, 0b011});
  ASSERT_NE(edge_ab, nullptr);
  ASSERT_EQ(edge_ab->predicates.size(), 2u);
  EXPECT_EQ(to_string(edge_ab->predicates.at(0)), "sum_a <= y1");
  EXPECT_EQ(to_string(edge_ab->predicates.at(1)), "x2 = y2");

  const auto edge_ac = _join_graph.find_edge(boost::dynamic_bitset<>{3, 0b101});
  ASSERT_NE(edge_ac, nullptr);
  ASSERT_EQ(edge_ac->predicates.size(), 1u);
  EXPECT_EQ(to_string(edge_ac->predicates.at(0)), "x2 <= z1");

  /**
   * Test parent relations
   */
  ASSERT_EQ(_join_graph.parent_relations.size(), 1u);
  EXPECT_EQ(_join_graph.parent_relations.at(0).parent, _projection_node_a);
  EXPECT_EQ(_join_graph.parent_relations.at(0).child_side, LQPChildSide::Left);

  /**
   * Test that the Vertices and Parents were properly dissected from the nodes that were turned into the JoinGraph
   */
  EXPECT_EQ(_join_graph.vertices.at(0)->parent_count(), 0u);
  EXPECT_EQ(_join_graph.vertices.at(1)->parent_count(), 0u);
  EXPECT_EQ(_join_graph.vertices.at(2)->parent_count(), 0u);
  EXPECT_EQ(_join_graph.parent_relations.at(0).parent->left_child(), nullptr);
}


}  // namespace opossum
