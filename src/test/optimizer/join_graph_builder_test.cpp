#include <memory>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinGraphBuilderTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    node_b = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "b");
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");
    b_c = node_a->get_column("c");
    c_a = node_c->get_column("a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b, b_c, c_a;
};

TEST_F(JoinGraphBuilderTest, None) {
  // Trying to create a JoinGraph from a vertex-type node gives std::nullopt

  // clang-format off
  const auto lqp =
  SortNode::make(expression_vector(a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(equals_(a_a, b_a),
      JoinNode::make(JoinMode::Inner, greater_than_(b_b, a_b),
        node_a,
        node_b)));
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_FALSE(join_graph);
}

TEST_F(JoinGraphBuilderTest, Basic) {
  // clang-format off
  const auto sort_node =
  SortNode::make(expression_vector(a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    node_a);

  const auto lqp =
  PredicateNode::make(equals_(b_a, c_a),
    PredicateNode::make(equals_(a_a, b_a),
      JoinNode::make(JoinMode::Inner, greater_than_(b_b, a_b),
        sort_node,
        JoinNode::make(JoinMode::Cross,
          node_b,
          node_c))));
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 3u);
  EXPECT_EQ(join_graph->vertices.at(0), sort_node);
  EXPECT_EQ(join_graph->vertices.at(1), node_b);
  EXPECT_EQ(join_graph->vertices.at(2), node_c);

  ASSERT_EQ(join_graph->edges.size(), 2u);

  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(3, 0b110));
  EXPECT_EQ(join_graph->edges.at(0).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(b_a, c_a));

  EXPECT_EQ(join_graph->edges.at(1).vertex_set, JoinGraphVertexSet(3, 0b011));
  EXPECT_EQ(join_graph->edges.at(1).predicates.size(), 2u);
  EXPECT_EQ(*join_graph->edges.at(1).predicates.at(0), *equals_(a_a, b_a));
  EXPECT_EQ(*join_graph->edges.at(1).predicates.at(1), *greater_than_(b_b, a_b));
}

TEST_F(JoinGraphBuilderTest, LocalPredicates) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(5, b_a),
    JoinNode::make(JoinMode::Inner, greater_than_(b_b, a_b),
      PredicateNode::make(greater_than_(a_a, 5),
        PredicateNode::make(less_than_(a_a, 10),
          node_a)),
      PredicateNode::make(less_than_equals_(b_b, 15),
        node_b)));
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 2u);
  EXPECT_EQ(join_graph->vertices.at(0), node_a);
  EXPECT_EQ(join_graph->vertices.at(1), node_b);

  ASSERT_EQ(join_graph->edges.size(), 3u);

  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(2, 0b10));
  EXPECT_EQ(join_graph->edges.at(0).predicates.size(), 2u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(5, b_a));
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(1), *less_than_equals_(b_b, 15));

  EXPECT_EQ(join_graph->edges.at(1).vertex_set, JoinGraphVertexSet(2, 0b11));
  EXPECT_EQ(join_graph->edges.at(1).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(1).predicates.at(0), *greater_than_(b_b, a_b));

  EXPECT_EQ(join_graph->edges.at(2).vertex_set, JoinGraphVertexSet(2, 0b01));
  EXPECT_EQ(join_graph->edges.at(2).predicates.size(), 2u);
  EXPECT_EQ(*join_graph->edges.at(2).predicates.at(0), *greater_than_(a_a, 5));
  EXPECT_EQ(*join_graph->edges.at(2).predicates.at(1), *less_than_(a_a, 10));
}

TEST_F(JoinGraphBuilderTest, ComputedExpression) {
  // An expression computed in one of the vertices is used in a predicate

  // clang-format off
  const auto group_by_expression = add_(b_a, b_b);
  const auto aggregate_node = AggregateNode::make(expression_vector(group_by_expression), expression_vector(), node_b);

  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, group_by_expression),
    JoinNode::make(JoinMode::Inner, equals_(c_a, a_a),
      node_c,
      node_a),
    aggregate_node);
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);
  ASSERT_EQ(join_graph->vertices.size(), 3u);
  EXPECT_EQ(join_graph->vertices.at(0), node_c);
  EXPECT_EQ(join_graph->vertices.at(1), node_a);
  EXPECT_EQ(join_graph->vertices.at(2), aggregate_node);

  ASSERT_EQ(join_graph->edges.size(), 2u);

  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(3, 0b110));
  EXPECT_EQ(join_graph->edges.at(0).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(a_a, group_by_expression));

  EXPECT_EQ(join_graph->edges.at(1).vertex_set, JoinGraphVertexSet(3, 0b011));
  EXPECT_EQ(join_graph->edges.at(1).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(1).predicates.at(0), *equals_(c_a, a_a));
}

TEST_F(JoinGraphBuilderTest, OuterJoin) {
  // Test that outer joins are treated as vertices

  // clang-format off
  const auto outer_join_node =
  JoinNode::make(JoinMode::Left, equals_(c_a, b_a),
    node_b,
    node_c);

  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    node_a,
    outer_join_node);
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 2u);
  EXPECT_EQ(join_graph->vertices.at(0), node_a);
  EXPECT_EQ(join_graph->vertices.at(1), outer_join_node);

  ASSERT_EQ(join_graph->edges.size(), 1u);
  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(2, 0b11));
  EXPECT_EQ(join_graph->edges.at(0).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(a_a, b_a));
}

TEST_F(JoinGraphBuilderTest, MultipleComponents) {
  // Test that components in the join graph get merged with a cross join

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Cross,
    node_a,
    node_b);
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 2u);
  EXPECT_EQ(join_graph->vertices.at(0), node_a);
  EXPECT_EQ(join_graph->vertices.at(1), node_b);

  ASSERT_EQ(join_graph->edges.size(), 1u);
  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(2, 0b11));
  EXPECT_EQ(join_graph->edges.at(0).predicates.size(), 0u);
}

TEST_F(JoinGraphBuilderTest, MultipleComponentsWithHyperEdge) {
  // Test that components in the join graph get merged with a cross join, even if they are connected by a hyperedge
  // DPccp needs the JoinGraph to be connected without relying on the hyperedges

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(add_(a_a, b_a), c_a),
    JoinNode::make(JoinMode::Cross,
      JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
         node_a,
         node_b),
      node_c));
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 3u);
  EXPECT_EQ(join_graph->vertices.at(0), node_a);
  EXPECT_EQ(join_graph->vertices.at(1), node_b);
  EXPECT_EQ(join_graph->vertices.at(2), node_c);

  ASSERT_EQ(join_graph->edges.size(), 3u);

  EXPECT_EQ(join_graph->edges.at(0).vertex_set, JoinGraphVertexSet(3, 0b111));
  ASSERT_EQ(join_graph->edges.at(0).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(add_(a_a, b_a), c_a));

  EXPECT_EQ(join_graph->edges.at(1).vertex_set, JoinGraphVertexSet(3, 0b011));
  ASSERT_EQ(join_graph->edges.at(1).predicates.size(), 1u);
  EXPECT_EQ(*join_graph->edges.at(1).predicates.at(0), *equals_(a_a, b_a));

  // The edge connecting the components of the JoinGraph can either be AC or BC. Depending on the hash function used
  // by the stdlib, either could happen
  const auto cross_edge = join_graph->edges.at(2);
  EXPECT_TRUE(cross_edge.vertex_set == JoinGraphVertexSet(3, 0b101) ||
              cross_edge.vertex_set == JoinGraphVertexSet(3, 0b110));
  ASSERT_EQ(join_graph->edges.at(2).predicates.size(), 0u);
}

TEST_F(JoinGraphBuilderTest, NonJoinGraphDisjunction) {
  // Unions are not represented in the JoinGraph

  // clang-format off
  const auto lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(equals_(a_a, 5),
      node_a),
    PredicateNode::make(greater_than_(a_b, 6),
      node_a));
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_FALSE(join_graph);
}

TEST_F(JoinGraphBuilderTest, NonJoinGraphJoin) {
  // Non-Inner/Cross Joins are not represented in the JoinGraph

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Left, equals_(a_a, b_a),
    node_a,
    node_b);
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);
  ASSERT_FALSE(join_graph);
}

TEST_F(JoinGraphBuilderTest, BuildAllInLQP) {
  // clang-format off
  const auto sub_lqp =
  AggregateNode::make(expression_vector(a_a), expression_vector(),
    JoinNode::make(JoinMode::Cross,
      node_a,
      node_b));

  const auto lqp =
  JoinNode::make(JoinMode::Cross,
    sub_lqp,
    node_c);
  // clang-format on

  const auto join_graphs = JoinGraph::build_all_in_lqp(lqp);

  ASSERT_EQ(join_graphs.size(), 2u);
  EXPECT_EQ(join_graphs.at(0).vertices.at(0), sub_lqp);
  EXPECT_EQ(join_graphs.at(0).vertices.at(1), node_c);
  EXPECT_EQ(join_graphs.at(1).vertices.at(0), node_a);
  EXPECT_EQ(join_graphs.at(1).vertices.at(1), node_b);
}

TEST_F(JoinGraphBuilderTest, MultiPredicateJoin) {
  const auto join_predicates =
      expression_vector(equals_(a_a, b_a), greater_than_(a_b, b_b), less_than_equals_(a_c, b_c));

  // clang-format off
  const auto full_outer_join_node =
  JoinNode::make(JoinMode::FullOuter, join_predicates,
    node_a,
    node_b);

  const auto lqp =
  JoinNode::make(JoinMode::Inner, join_predicates,
    node_a,
    full_outer_join_node);
  // clang-format on

  const auto join_graph = JoinGraphBuilder()(lqp);

  ASSERT_TRUE(join_graph);

  ASSERT_EQ(join_graph->vertices.size(), 2u);
  EXPECT_EQ(join_graph->vertices.at(0), node_a);
  EXPECT_EQ(join_graph->vertices.at(1), full_outer_join_node);

  ASSERT_EQ(join_graph->edges.size(), 1u);

  ASSERT_EQ(join_graph->edges.at(0).predicates.size(), 3u);
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(0), *equals_(a_a, b_a));
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(1), *greater_than_(a_b, b_b));
  EXPECT_EQ(*join_graph->edges.at(0).predicates.at(2), *less_than_equals_(a_c, b_c));
}

}  // namespace opossum
