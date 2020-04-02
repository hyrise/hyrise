#include <sstream>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinGraphTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "d");

    a_a = node_a->get_column("a");
    b_a = node_b->get_column("a");
    c_a = node_c->get_column("a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  LQPColumnReference a_a, b_a, c_a;
};

TEST_F(JoinGraphTest, FindPredicates) {
  const auto edge_a_b = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0011}, {equals_(a_a, b_a)}};
  const auto edge_b_c = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0110}, {equals_(b_a, c_a), less_than_(b_a, c_a)}};
  const auto edge_b = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0010}, {equals_(b_a, 3)}};
  const auto edge_c_d = JoinGraphEdge{JoinGraphVertexSet{4u, 0b1100}, {}};

  const auto join_graph = JoinGraph{{node_a, node_b, node_c, node_d}, {edge_a_b, edge_b_c, edge_c_d, edge_b}};

  EXPECT_EQ(join_graph.find_local_predicates(0).size(), 0u);
  ASSERT_EQ(join_graph.find_local_predicates(1).size(), 1u);
  EXPECT_EQ(*join_graph.find_local_predicates(1).at(0), *equals_(b_a, 3));

  const auto predicates_ab_c = join_graph.find_join_predicates({4, 0b1100}, {4, 0b0011});
  ASSERT_EQ(predicates_ab_c.size(), 2u);
  EXPECT_EQ(*predicates_ab_c.at(0), *equals_(b_a, c_a));
  EXPECT_EQ(*predicates_ab_c.at(1), *less_than_(b_a, c_a));

  const auto predicates_a_bcd = join_graph.find_join_predicates({4, 0b1110}, {4, 0b0001});
  ASSERT_EQ(predicates_a_bcd.size(), 1u);
  EXPECT_EQ(*predicates_a_bcd.at(0), *equals_(a_a, b_a));

  const auto predicates_b = join_graph.find_local_predicates(1);
  ASSERT_EQ(predicates_b.size(), 1u);
  EXPECT_EQ(*predicates_b.at(0), *equals_(b_a, 3));

  EXPECT_TRUE(join_graph.find_local_predicates(0).empty());
}

TEST_F(JoinGraphTest, OutputToStream) {
  const auto edge_a_b = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0011}, {equals_(a_a, b_a)}};
  const auto edge_b_c = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0110}, {equals_(b_a, c_a), less_than_(b_a, c_a)}};
  const auto edge_b = JoinGraphEdge{JoinGraphVertexSet{4u, 0b0010}, {equals_(b_a, 3)}};
  const auto edge_c_d = JoinGraphEdge{JoinGraphVertexSet{4u, 0b1100}, {}};

  const auto join_graph = JoinGraph{{node_a, node_b, node_c, node_d}, {edge_a_b, edge_b_c, edge_c_d, edge_b}};

  auto stream = std::stringstream{};
  stream << join_graph;

  EXPECT_EQ(stream.str(), R"(==== Vertices ====
[MockNode 'a'] Columns: a | pruned: 0/1 columns
[MockNode 'b'] Columns: a | pruned: 0/1 columns
[MockNode 'c'] Columns: a | pruned: 0/1 columns
[MockNode 'd'] Columns: a | pruned: 0/1 columns
===== Edges ======
Vertices: 0011; 1 predicates
a = a
Vertices: 0110; 2 predicates
a = a
a < a
Vertices: 1100; 0 predicates
Vertices: 0010; 1 predicates
a = 3
)");
}

}  // namespace opossum
