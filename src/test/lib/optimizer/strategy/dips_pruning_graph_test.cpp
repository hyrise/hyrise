#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "lib/optimizer/strategy/strategy_base_test.hpp"
#include "utils/assert.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/get_table.hpp"
#include "optimizer/strategy/dips_pruning_graph.hpp"
#include "optimizer/strategy/dips_pruning_graph_edge.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DipsPruningGraphTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    auto int_float2_table = load_table("resources/test_data/tbl/int_float2.tbl", 2u);
    ChunkEncoder::encode_all_chunks(int_float2_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("int_float2", int_float2_table);

    auto int_float2_sorted_table = load_table("resources/test_data/tbl/int_float2_sorted.tbl", 2u);
    ChunkEncoder::encode_all_chunks(int_float2_sorted_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("int_float2_sorted", int_float2_sorted_table);

    auto int_float2_sorted_mixed_table = load_table("resources/test_data/tbl/int_float2_sorted_mixed.tbl", 2u);
    ChunkEncoder::encode_all_chunks(int_float2_sorted_mixed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("int_float2_sorted_mixed", int_float2_sorted_mixed_table);
  }
};

TEST_F(DipsPruningGraphTest, DipsJoinGraphIsEmpty) {
  auto graph = DipsPruningGraph{};
  EXPECT_TRUE(graph.empty());
}

TEST_F(DipsPruningGraphTest, BuildJoinGraph) {
  // We are expecting the build function to transform the following LQP:
  //            |><|
  //           A.a=B.a
  //           /    \
  //         /        \
  //       /            \
  //     A              |><|
  //                   B.b=C.b
  //                   /    \
  //                 /        \
  //               /            \
  //             B                C
  // to the following graph:
  //                     B=0
  //                   /    \
  //         B.b=C.b /        \ A.a=B.a
  //               /            \
  //            C=1              A=2
  // vertices: [B,C,A]
  // edges: [({0,1}, B.b=C.b), ({0,2}, A.a=B.a)]

  const auto stored_table_node_a = StoredTableNode::make("int_float2");
  const auto stored_table_node_b = StoredTableNode::make("int_float2_sorted");
  const auto stored_table_node_c = StoredTableNode::make("int_float2_sorted_mixed");

  std::shared_ptr<LQPColumnExpression> a_a, b_a, b_b, c_b;

  a_a = stored_table_node_a->get_column("a");
  b_a = stored_table_node_b->get_column("a");
  b_b = stored_table_node_b->get_column("b");
  c_b = stored_table_node_c->get_column("b");

  const auto join_node_b_c =
      JoinNode::make(JoinMode::Inner, equals_(b_b, c_b), stored_table_node_b, stored_table_node_c);
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(a_a, b_a), stored_table_node_a, join_node_b_c);
  auto graph = DipsPruningGraph{};
  graph.build_graph(input_lqp);

  std::vector<std::shared_ptr<StoredTableNode>> vertices{stored_table_node_b, stored_table_node_c, stored_table_node_a};
  auto expected_edge_vertex_set_b_c = std::set<size_t>{0, 1};
  auto expected_edge_predicate_b_c =
      std::dynamic_pointer_cast<BinaryPredicateExpression>(join_node_b_c->join_predicates()[0]);
  auto expected_edge_vertex_set_a_b = std::set<size_t>{2, 0};
  auto expected_edge_predicate_a_b =
      std::dynamic_pointer_cast<BinaryPredicateExpression>(input_lqp->join_predicates()[0]);

  EXPECT_EQ(graph.vertices[0], vertices[0]);
  EXPECT_EQ(graph.vertices[1], vertices[1]);
  EXPECT_EQ(graph.vertices[2], vertices[2]);

  // We only have one predicate per edge in this test scenario.
  EXPECT_EQ(graph.edges[0].vertex_set, expected_edge_vertex_set_b_c);
  EXPECT_EQ(graph.edges[0].predicates[0], expected_edge_predicate_b_c);

  EXPECT_EQ(graph.edges[1].vertex_set, expected_edge_vertex_set_a_b);
  EXPECT_EQ(graph.edges[1].predicates[0], expected_edge_predicate_a_b);
}

TEST_F(DipsPruningGraphTest, JoinGraphIsTree) {
  auto graph = DipsPruningGraph{};
  auto edge_a_b = DipsPruningGraphEdge{std::set<size_t>{0, 1}, nullptr};
  auto edge_a_c = DipsPruningGraphEdge{std::set<size_t>{0, 2}, nullptr};
  auto edge_c_d = DipsPruningGraphEdge{std::set<size_t>{2, 3}, nullptr};
  graph.edges.push_back(edge_a_b);
  graph.edges.push_back(edge_a_c);
  graph.edges.push_back(edge_c_d);

  EXPECT_TRUE(graph.is_tree());
}

TEST_F(DipsPruningGraphTest, DipsJoinGraphIsNoTree) {
  auto graph = DipsPruningGraph{};
  auto edge_a_b = DipsPruningGraphEdge{std::set<size_t>{0, 1}, nullptr};
  auto edge_a_c = DipsPruningGraphEdge{std::set<size_t>{0, 2}, nullptr};
  auto edge_c_b = DipsPruningGraphEdge{std::set<size_t>{2, 1}, nullptr};
  graph.edges.push_back(edge_a_b);
  graph.edges.push_back(edge_a_c);
  graph.edges.push_back(edge_c_b);

  EXPECT_FALSE(graph.is_tree());
}

TEST_F(DipsPruningGraphTest, DipsJoinGraphTraversal) {
  // We are traversing the following tree:
  //             0
  //           /    \
//          1      2
  //        /    \      \
//       3      4      5
  auto graph = DipsPruningGraph{};
  auto edge_0_1 = DipsPruningGraphEdge{std::set<size_t>{0, 1}, nullptr};
  auto edge_0_2 = DipsPruningGraphEdge{std::set<size_t>{0, 2}, nullptr};
  auto edge_1_3 = DipsPruningGraphEdge{std::set<size_t>{1, 3}, nullptr};
  auto edge_1_4 = DipsPruningGraphEdge{std::set<size_t>{1, 4}, nullptr};
  auto edge_2_5 = DipsPruningGraphEdge{std::set<size_t>{2, 5}, nullptr};
  graph.edges.push_back(edge_0_1);
  graph.edges.push_back(edge_0_2);
  graph.edges.push_back(edge_1_3);
  graph.edges.push_back(edge_1_4);
  graph.edges.push_back(edge_2_5);

  auto top_down_result = graph.top_down_traversal();

  EXPECT_EQ(top_down_result[0].vertex_set, edge_0_1.vertex_set);
  EXPECT_EQ(top_down_result[1].vertex_set, edge_1_3.vertex_set);
  EXPECT_EQ(top_down_result[2].vertex_set, edge_1_4.vertex_set);
  EXPECT_EQ(top_down_result[3].vertex_set, edge_0_2.vertex_set);
  EXPECT_EQ(top_down_result[4].vertex_set, edge_2_5.vertex_set);

  auto bottom_up_result = graph.bottom_up_traversal();

  EXPECT_EQ(bottom_up_result[0].vertex_set, edge_1_3.vertex_set);
  EXPECT_EQ(bottom_up_result[1].vertex_set, edge_1_4.vertex_set);
  EXPECT_EQ(bottom_up_result[2].vertex_set, edge_0_1.vertex_set);
  EXPECT_EQ(bottom_up_result[3].vertex_set, edge_2_5.vertex_set);
  EXPECT_EQ(bottom_up_result[4].vertex_set, edge_0_2.vertex_set);
}

}  // namespace opossum