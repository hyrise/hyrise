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
#include "optimizer/strategy/dips_pruning_rule.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DipsPruningRuleTestClass : DipsPruningRule {
 public:
  template <typename COLUMN_TYPE>
  bool _range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a,
                        std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const {
    return DipsPruningRule::_range_intersect<COLUMN_TYPE>(range_a, range_b);
  }

  template <typename COLUMN_TYPE>
  std::set<ChunkID> _calculate_pruned_chunks(
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> base_chunk_ranges,
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> partner_chunk_ranges) const {
    return DipsPruningRule::_calculate_pruned_chunks<COLUMN_TYPE>(base_chunk_ranges, partner_chunk_ranges);
  }

  void _bottom_up_dip_traversal(std::shared_ptr<DipsJoinGraphNode> node) const {
    return DipsPruningRule::_bottom_up_dip_traversal(node);
  }

  void _top_down_dip_traversal(std::shared_ptr<DipsJoinGraphNode> node) const {
    return DipsPruningRule::_top_down_dip_traversal(node);
  }
};

class DipsPruningRuleTest : public StrategyBaseTest {
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

    _real_rule = std::make_shared<DipsPruningRule>();
    _rule = std::make_shared<DipsPruningRuleTestClass>();
  }

  std::shared_ptr<DipsPruningRuleTestClass> _rule;
  std::shared_ptr<DipsPruningRule> _real_rule;
};

TEST_F(DipsPruningRuleTest, RangeIntersectionTest) {
  // int32_t

  std::pair<int32_t, int32_t> first_range(1, 2);
  std::pair<int32_t, int32_t> second_range(3, 4);

  bool result_1 = _rule->_range_intersect<int32_t>(first_range, second_range);
  bool result_2 = _rule->_range_intersect<int32_t>(second_range, first_range);

  EXPECT_FALSE(result_1);
  EXPECT_FALSE(result_2);

  first_range = std::pair<int32_t, int32_t>(1, 8);
  second_range = std::pair<int32_t, int32_t>(3, 6);

  result_1 = _rule->_range_intersect<int32_t>(first_range, second_range);
  result_2 = _rule->_range_intersect<int32_t>(second_range, first_range);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);

  first_range = std::pair<int32_t, int32_t>(1, 8);
  second_range = std::pair<int32_t, int32_t>(0, 1);

  result_1 = _rule->_range_intersect<int32_t>(first_range, second_range);
  result_2 = _rule->_range_intersect<int32_t>(second_range, first_range);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);

  // double

  std::pair<double, double> first_range_double(1.4, 2.3);
  std::pair<double, double> second_range_double(3.3, 4.5);

  result_1 = _rule->_range_intersect<double>(first_range_double, second_range_double);
  result_2 = _rule->_range_intersect<double>(second_range_double, first_range_double);

  EXPECT_FALSE(result_1);
  EXPECT_FALSE(result_2);

  first_range_double = std::pair<double, double>(2.1, 8.4);
  second_range_double = std::pair<double, double>(3.4, 6.9);

  result_1 = _rule->_range_intersect<double>(first_range_double, second_range_double);
  result_2 = _rule->_range_intersect<double>(second_range_double, first_range_double);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);

  first_range_double = std::pair<double, double>(1.0, 8.0);
  second_range_double = std::pair<double, double>(0.0, 1.0);

  result_1 = _rule->_range_intersect<double>(first_range_double, second_range_double);
  result_2 = _rule->_range_intersect<double>(second_range_double, first_range_double);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);

  // pmr_string

  std::pair<pmr_string, pmr_string> first_range_string("aa", "bb");
  std::pair<pmr_string, pmr_string> second_range_string("cc", "dd");

  result_1 = _rule->_range_intersect<pmr_string>(first_range_string, second_range_string);
  result_2 = _rule->_range_intersect<pmr_string>(second_range_string, first_range_string);

  EXPECT_FALSE(result_1);
  EXPECT_FALSE(result_2);

  first_range_string = std::pair<pmr_string, pmr_string>("aa", "gg");
  second_range_string = std::pair<pmr_string, pmr_string>("cc", "ee");

  result_1 = _rule->_range_intersect<pmr_string>(first_range_string, second_range_string);
  result_2 = _rule->_range_intersect<pmr_string>(second_range_string, first_range_string);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);

  first_range_string = std::pair<pmr_string, pmr_string>("cc", "ff");
  second_range_string = std::pair<pmr_string, pmr_string>("aa", "cc");

  result_1 = _rule->_range_intersect<pmr_string>(first_range_string, second_range_string);
  result_2 = _rule->_range_intersect<pmr_string>(second_range_string, first_range_string);

  EXPECT_TRUE(result_1);
  EXPECT_TRUE(result_2);
}

TEST_F(DipsPruningRuleTest, CalculatePrunedChunks) {
  std::map<ChunkID, std::vector<std::pair<int32_t, int32_t>>> base_ranges{
      {ChunkID{0}, std::vector{std::pair<int32_t, int32_t>(1, 5)}},
      {ChunkID{1}, std::vector{std::pair<int32_t, int32_t>(8, 10)}},
      {ChunkID{2}, std::vector{std::pair<int32_t, int32_t>(10, 12)}}};
  std::map<ChunkID, std::vector<std::pair<int32_t, int32_t>>> partner_ranges{
      {ChunkID{0}, std::vector{std::pair<int32_t, int32_t>(6, 7)}},  // raus
      {ChunkID{1}, std::vector{std::pair<int32_t, int32_t>(9, 11)}},
      {ChunkID{2}, std::vector{std::pair<int32_t, int32_t>(12, 16)}}};

  auto pruned_chunks = _rule->_calculate_pruned_chunks<int32_t>(base_ranges, partner_ranges);
  std::set<ChunkID> expected_pruned_chunk_ids{ChunkID{0}};

  EXPECT_EQ(pruned_chunks.size(), 1);
  EXPECT_TRUE((pruned_chunks == expected_pruned_chunk_ids));
}

TEST_F(DipsPruningRuleTest, ApplyPruningSimple) {
  // LEFT -> RIGHT
  auto stored_table_node_1 = std::make_shared<StoredTableNode>("int_float2_sorted");
  auto stored_table_node_2 = std::make_shared<StoredTableNode>("int_float2");
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, equals_(lqp_column_(stored_table_node_2, ColumnID{0}),
                                                                       lqp_column_(stored_table_node_1, ColumnID{0})));
  join_node->set_left_input(stored_table_node_1);
  join_node->set_right_input(stored_table_node_2);

  std::vector<ChunkID> pruned_chunk_ids{ChunkID{1}};
  stored_table_node_2->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));

  StrategyBaseTest::apply_rule(_real_rule, join_node);

  std::vector<ChunkID> expected_pruned_ids_right{ChunkID{0}, ChunkID{2}, ChunkID{3}};

  EXPECT_EQ(stored_table_node_1->pruned_chunk_ids(), expected_pruned_ids_right);

  // RIGHT -> LEFT

  stored_table_node_2->set_pruned_chunk_ids(std::vector<ChunkID>());
  stored_table_node_1->set_pruned_chunk_ids(std::vector<ChunkID>{ChunkID{0}, ChunkID{2}, ChunkID{3}});

  join_node = std::make_shared<JoinNode>(JoinMode::Inner, equals_(lqp_column_(stored_table_node_1, ColumnID{0}),
                                                                  lqp_column_(stored_table_node_2, ColumnID{0})));

  join_node->set_left_input(stored_table_node_2);
  join_node->set_right_input(stored_table_node_1);

  StrategyBaseTest::apply_rule(_real_rule, join_node);

  std::vector<ChunkID> expected_pruned_ids_left{ChunkID{1}};

  EXPECT_EQ(stored_table_node_2->pruned_chunk_ids(), expected_pruned_ids_left);
}

TEST_F(DipsPruningRuleTest, DipsJoinGraphIsEmpty) {
  std::shared_ptr<DipsJoinGraph> join_graph = std::make_shared<DipsJoinGraph>();

  EXPECT_TRUE(join_graph->is_empty());
}

TEST_F(DipsPruningRuleTest, BuildJoinGraph) {
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

  const auto join_node_b_c = JoinNode::make(JoinMode::Inner, equals_(b_b,c_b), stored_table_node_b, stored_table_node_c);
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(a_a,b_a), stored_table_node_a, join_node_b_c);
  auto graph = Graph{};
  graph.build_graph(input_lqp);

  std::vector<std::shared_ptr<StoredTableNode>> vertices { stored_table_node_b,  stored_table_node_c, stored_table_node_a};
  auto expected_edge_vertex_set_b_c = std::set<size_t>{0,1};
  auto expected_edge_predicate_b_c = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_node_b_c->join_predicates()[0]);
  auto expected_edge_vertex_set_a_b= std::set<size_t>{2,0};
  auto expected_edge_predicate_a_b = std::dynamic_pointer_cast<BinaryPredicateExpression>(input_lqp->join_predicates()[0]);

  EXPECT_EQ(graph.vertices[0], vertices[0]);
  EXPECT_EQ(graph.vertices[1], vertices[1]);
  EXPECT_EQ(graph.vertices[2], vertices[2]);

  // We only have one predicate per edge in this test scenario.
  EXPECT_EQ(graph.edges[0].vertex_set, expected_edge_vertex_set_b_c);
  EXPECT_EQ(graph.edges[0].predicates[0], expected_edge_predicate_b_c);

  EXPECT_EQ(graph.edges[1].vertex_set, expected_edge_vertex_set_a_b);
  EXPECT_EQ(graph.edges[1].predicates[0], expected_edge_predicate_a_b);
}

TEST_F(DipsPruningRuleTest, JoinGraphIsTree) {
  auto graph = Graph{};
  auto edge_a_b = Graph::JoinGraphEdge{std::set<size_t>{0,1}, nullptr};
  auto edge_a_c = Graph::JoinGraphEdge{std::set<size_t>{0,2}, nullptr};
  auto edge_c_d = Graph::JoinGraphEdge{std::set<size_t>{2,3}, nullptr};
  graph.edges.push_back(edge_a_b);
  graph.edges.push_back(edge_a_c);
  graph.edges.push_back(edge_c_d);

  EXPECT_TRUE(graph.is_tree());
}

TEST_F(DipsPruningRuleTest, DipsJoinGraphIsNoTree) {
  auto graph = Graph{};
  auto edge_a_b = Graph::JoinGraphEdge{std::set<size_t>{0,1}, nullptr};
  auto edge_a_c = Graph::JoinGraphEdge{std::set<size_t>{0,2}, nullptr};
  auto edge_c_b = Graph::JoinGraphEdge{std::set<size_t>{2,1}, nullptr};
  graph.edges.push_back(edge_a_b);
  graph.edges.push_back(edge_a_c);
  graph.edges.push_back(edge_c_b);

  EXPECT_FALSE(graph.is_tree());
}

TEST_F(DipsPruningRuleTest, DipsJoinGraphTraversal) {
// We are traversing the following tree:
//             0
//           /    \
//          1      2
//        /    \      \
//       3      4      5
  auto graph = Graph{};
  auto edge_0_1 = Graph::JoinGraphEdge{std::set<size_t>{0,1}, nullptr};
  auto edge_0_2 = Graph::JoinGraphEdge{std::set<size_t>{0,2}, nullptr};
  auto edge_1_3 = Graph::JoinGraphEdge{std::set<size_t>{1,3}, nullptr};
  auto edge_1_4 = Graph::JoinGraphEdge{std::set<size_t>{1,4}, nullptr};
  auto edge_2_5 = Graph::JoinGraphEdge{std::set<size_t>{2,5}, nullptr};
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

// TEST_F(DipsPruningRuleTest, DipsJoinGraphTraversal) {
//   // [table1 <-> table2 <-> table3] cycle free structure
//   std::shared_ptr<StoredTableNode> table1 = std::make_shared<StoredTableNode>("int_float2");
//   std::shared_ptr<StoredTableNode> table2 = std::make_shared<StoredTableNode>("int_float2_sorted");
//   std::shared_ptr<StoredTableNode> table3 = std::make_shared<StoredTableNode>("int_float2_sorted_mixed");

//   std::vector<ChunkID> table1_pruned_chunk_ids{};
//   std::vector<ChunkID> table2_pruned_chunk_ids{ChunkID{0}};
//   std::vector<ChunkID> table3_pruned_chunk_ids{};

//   table1->set_pruned_chunk_ids(std::vector<ChunkID>(table1_pruned_chunk_ids.begin(), table1_pruned_chunk_ids.end()));
//   table2->set_pruned_chunk_ids(std::vector<ChunkID>(table2_pruned_chunk_ids.begin(), table2_pruned_chunk_ids.end()));
//   table3->set_pruned_chunk_ids(std::vector<ChunkID>(table3_pruned_chunk_ids.begin(), table3_pruned_chunk_ids.end()));

//   std::shared_ptr<DipsJoinGraph> join_graph = std::make_shared<DipsJoinGraph>();  // build dips join graph

//   std::shared_ptr<DipsJoinGraphNode> table1_node = join_graph->get_node_for_table(table1);
//   std::shared_ptr<DipsJoinGraphNode> table2_node = join_graph->get_node_for_table(table2);
//   std::shared_ptr<DipsJoinGraphNode> table3_node = join_graph->get_node_for_table(table3);

//   std::shared_ptr<DipsJoinGraphEdge> table1_to_table2_edge =
//       table1_node->get_edge_for_table(table2_node);  // set int_float2 JOIN int_float2_sorted ON a=a
//   std::shared_ptr<DipsJoinGraphEdge> table2_to_table1_edge = table2_node->get_edge_for_table(table1_node);

//   table1_to_table2_edge->append_predicate(equals_(lqp_column_(table1, ColumnID{0}), lqp_column_(table2, ColumnID{0})));
//   table2_to_table1_edge->append_predicate(equals_(lqp_column_(table1, ColumnID{0}), lqp_column_(table2, ColumnID{0})));

//   std::shared_ptr<DipsJoinGraphEdge> table2_to_table3_edge =
//       table2_node->get_edge_for_table(table3_node);  // set int_float2 JOIN int_float2_sorted ON b=b
//   std::shared_ptr<DipsJoinGraphEdge> table3_to_table2_edge = table3_node->get_edge_for_table(table2_node);
//   table2_to_table3_edge->append_predicate(equals_(lqp_column_(table2, ColumnID{1}), lqp_column_(table3, ColumnID{1})));
//   table3_to_table2_edge->append_predicate(equals_(lqp_column_(table2, ColumnID{1}), lqp_column_(table3, ColumnID{1})));

//   EXPECT_TRUE(join_graph->is_tree());

//   join_graph->set_root(table1_node);  // prune based on dips
//   _rule->_bottom_up_dip_traversal(table1_node);

//   std::vector<ChunkID> expected_table1_pruned_ids{ChunkID{1}};
//   std::vector<ChunkID> expected_table2_pruned_ids{ChunkID{0}, ChunkID{2}, ChunkID{3}};
//   std::vector<ChunkID> expected_table3_pruned_ids{ChunkID{0}};

//   EXPECT_EQ(table1->pruned_chunk_ids(), expected_table1_pruned_ids);
//   EXPECT_EQ(table2->pruned_chunk_ids(), expected_table2_pruned_ids);
//   EXPECT_EQ(table3->pruned_chunk_ids(), expected_table3_pruned_ids);

//   _rule->_top_down_dip_traversal(table1_node);

//   expected_table1_pruned_ids = std::vector<ChunkID>{ChunkID{1}};
//   expected_table2_pruned_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}, ChunkID{3}};
//   expected_table3_pruned_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}, ChunkID{3}};

//   EXPECT_EQ(table1->pruned_chunk_ids(), expected_table1_pruned_ids);
//   EXPECT_EQ(table2->pruned_chunk_ids(), expected_table2_pruned_ids);
//   EXPECT_EQ(table3->pruned_chunk_ids(), expected_table3_pruned_ids);
// }

}  // namespace opossum
