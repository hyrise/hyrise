#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "optimizer/join_graph.hpp"
#include "optimizer/join_graph_builder.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class JoinGraphTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int.tbl", 0));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_int.tbl", 0));
    StorageManager::get().add_table("table_c", load_table("src/test/tables/int_int_int.tbl", 0));
  }

  void TearDown() override { StorageManager::get().reset(); }
};

TEST_F(JoinGraphTest, BuildJoinGraphSimple) {
  /**
   *      --Join (table_a.a = table_b.b) --
   *    /                                  \
   *  table_a                               table_b
   */
  auto join_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);
  auto table_a_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_node = std::make_shared<StoredTableNode>("table_b");

  join_node->set_left_child(table_a_node);
  join_node->set_right_child(table_b_node);

  const auto join_graph = JoinGraphBuilder::build_join_graph(join_node);

  EXPECT_VERTEX_NODES(join_graph, std::vector<std::shared_ptr<AbstractASTNode>>({table_a_node, table_b_node}));

  EXPECT_EQ(join_graph->edges().size(), 1u);
  EXPECT_JOIN_EDGE(join_graph, table_a_node, table_b_node, ColumnID{0}, ColumnID{1}, ScanType::OpEquals);
}

TEST_F(JoinGraphTest, BuildJoinGraphMedium) {
  /**
   * Test that
   *    - Multiple joins are detected
   *    - A PredicateNode is opaque
   */

  /**
   *                   Projection
   *                     |
   *     Join_a (table_a.a > table_c.b)
   *    /                              \
   *  table_a                   Join_b (table_b.a = table_c.c)
   *                           /                              \
   *                     table_b                              Predicate (a > 4)
   *                                                              |
   *                                                            table_c
   */
  auto projection_node = std::make_shared<ProjectionNode>(Expression::create_columns({ColumnID{0}}));
  auto join_a_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{3}), ScanType::OpGreaterThan);
  auto join_b_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{2}), ScanType::OpEquals);
  auto table_a_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_node = std::make_shared<StoredTableNode>("table_b");
  auto table_c_node = std::make_shared<StoredTableNode>("table_c");
  auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 4);

  predicate_node->set_left_child(table_c_node);
  join_b_node->set_left_child(table_b_node);
  join_b_node->set_right_child(predicate_node);
  join_a_node->set_left_child(table_a_node);
  join_a_node->set_right_child(join_b_node);
  projection_node->set_left_child(join_a_node);

  // Searching from the root shouldn't yield anything except the root as it is not a join
  const auto join_graph_a = JoinGraphBuilder::build_join_graph(projection_node);
  EXPECT_VERTEX_NODES(join_graph_a, std::vector<std::shared_ptr<AbstractASTNode>>({projection_node}));
  EXPECT_EQ(join_graph_a->edges().size(), 0u);

  // Searching from join_a should yield a non-empty join graph
  const auto join_graph_b = JoinGraphBuilder::build_join_graph(join_a_node);

  EXPECT_VERTEX_NODES(join_graph_b,
                      std::vector<std::shared_ptr<AbstractASTNode>>({table_a_node, table_b_node, table_c_node}));

  EXPECT_EQ(join_graph_b->edges().size(), 2u);
  EXPECT_JOIN_EDGE(join_graph_b, table_a_node, table_c_node, ColumnID{0}, ColumnID{1}, ScanType::OpGreaterThan);
  EXPECT_JOIN_EDGE(join_graph_b, table_b_node, table_c_node, ColumnID{0}, ColumnID{2}, ScanType::OpEquals);
  EXPECT_VERTEX_PREDICATE(join_graph_b, table_c_node, ColumnID{0}, ScanType::OpGreaterThan, 4);
}

TEST_F(JoinGraphTest, BuildJoinGraphLarge) {
  /**
   * Test that
   *    - a graph with multiple nested joins is parsed correctly
   *    - a CrossJoin is resolved correctly
   */

  /**
   * Involves a cross join and joins that take join results as input
   *
   *     Join_a (table_a_0.a > table_c_1.b)
   *    /                              \
   *  table_a_0                 Join_b (table_b_0.b = table_c_1.c)
   *                           /                              \
   *       Join_c (table_b_0.a = table_c_0.c)                 Join_d (table_b_1.a < table_c_1.a)
   *       /                           \                      /                             \
   * table_b_0                         table_c_0       table_b_1                           CrossJoin
   *                                                                                        /       \
   *                                                                                  table_a_1      table_c_1
   */
  auto join_a_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{9}), ScanType::OpGreaterThan);
  auto join_b_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{1}, ColumnID{5}), ScanType::OpEquals);
  auto join_c_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{2}), ScanType::OpEquals);
  auto join_d_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);
  auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  auto table_a_0_node = std::make_shared<StoredTableNode>("table_a");
  auto table_a_1_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_0_node = std::make_shared<StoredTableNode>("table_b");
  auto table_b_1_node = std::make_shared<StoredTableNode>("table_b");
  auto table_c_0_node = std::make_shared<StoredTableNode>("table_c");
  auto table_c_1_node = std::make_shared<StoredTableNode>("table_c");

  cross_join_node->set_children(table_a_1_node, table_c_1_node);
  join_d_node->set_children(table_b_1_node, cross_join_node);
  join_c_node->set_children(table_b_0_node, table_c_0_node);
  join_b_node->set_children(join_c_node, join_d_node);
  join_a_node->set_children(table_a_0_node, join_b_node);

  // Searching from join_a should yield a non-empty join graph
  const auto join_graph = JoinGraphBuilder::build_join_graph(join_a_node);

  EXPECT_VERTEX_NODES(join_graph,
                      std::vector<std::shared_ptr<AbstractASTNode>>({table_a_0_node, table_b_0_node, table_c_0_node,
                                                                     table_b_1_node, table_a_1_node, table_c_1_node}));

  EXPECT_EQ(join_graph->edges().size(), 5u);
  EXPECT_JOIN_EDGE(join_graph, table_a_0_node, table_c_1_node, ColumnID{0}, ColumnID{1}, ScanType::OpGreaterThan);
  EXPECT_JOIN_EDGE(join_graph, table_b_0_node, table_c_1_node, ColumnID{1}, ColumnID{2}, ScanType::OpEquals);
  EXPECT_JOIN_EDGE(join_graph, table_b_0_node, table_c_0_node, ColumnID{0}, ColumnID{2}, ScanType::OpEquals);
  EXPECT_JOIN_EDGE(join_graph, table_b_1_node, table_c_1_node, ColumnID{0}, ColumnID{0}, ScanType::OpLessThan);
  EXPECT_CROSS_JOIN_EDGE(join_graph, table_a_1_node, table_c_1_node);
}

TEST_F(JoinGraphTest, BuildJoinGraphMediumWithPredicates) {
  /**
   * Predicates are edges as well, if they compare two columns.
   *
   * Test that
   *    - Predicate_a is recognized as an Edge, because it operates on two columns
   *    - Predicate_a is not an edge, but becomes a vertex instead.
   */

  /**
   *                   Projection
   *                     |
   *     ____Join_a (table_a.a > table_c.b)____
   *    /                                      \
   * table_a                  Predicate_a (table_c.a < table_b.a)
   *                                           |
   *                              Join_b (table_b.a = table_c.c)
   *                             /                              \
   *                      table_b                          Predicate_b (a > 4)
   *                                                             |
   *                                                           table_c
   */
  auto projection_node = std::make_shared<ProjectionNode>(Expression::create_columns({ColumnID{0}}));
  auto join_a_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{3}), ScanType::OpGreaterThan);
  auto join_b_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{2}), ScanType::OpEquals);
  auto table_a_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_node = std::make_shared<StoredTableNode>("table_b");
  auto table_c_node = std::make_shared<StoredTableNode>("table_c");
  auto predicate_a_node = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpLessThan, ColumnID{0});
  auto predicate_b_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 4);

  predicate_a_node->set_left_child(join_b_node);
  predicate_b_node->set_left_child(table_c_node);
  join_b_node->set_left_child(table_b_node);
  join_b_node->set_right_child(predicate_b_node);
  join_a_node->set_left_child(table_a_node);
  join_a_node->set_right_child(predicate_a_node);
  projection_node->set_left_child(join_a_node);

  // Searching from join_a should yield a non-empty join graph
  const auto join_graph = JoinGraphBuilder::build_join_graph(join_a_node);

  EXPECT_VERTEX_NODES(join_graph,
                      std::vector<std::shared_ptr<AbstractASTNode>>({table_a_node, table_b_node, table_c_node}));

  EXPECT_EQ(join_graph->edges().size(), 3u);
  EXPECT_JOIN_EDGE(join_graph, table_a_node, table_c_node, ColumnID{0}, ColumnID{1}, ScanType::OpGreaterThan);
  EXPECT_JOIN_EDGE(join_graph, table_b_node, table_c_node, ColumnID{0}, ColumnID{2}, ScanType::OpEquals);
  EXPECT_JOIN_EDGE(join_graph, table_b_node, table_c_node, ColumnID{0}, ColumnID{0}, ScanType::OpGreaterThan);
  EXPECT_VERTEX_PREDICATE(join_graph, table_c_node, ColumnID{0}, ScanType::OpGreaterThan, 4);
}

TEST_F(JoinGraphTest, BuildJoinGraphMediumWithPredicatesAndCrossJoin) {
  /**
   * Test that
   *    - Joins, Column-to-Column Predicates and Cross Joins work in combination
   */

  /**
   *                   Projection
   *                     |
   *           Predicate_a (table_b.b = 3)
   *                     |
   *     _____Join (table_a.a > table_c.b)_____
   *    /                                      \
   * table_a                  Predicate_b (table_c.a < table_b.a)
   *                                           |
   *                              _________CrossJoin____________
   *                             /                              \
   *                      table_b                     Predicate_c (table_c.a > 4)
   *                                                             |
   *                                                           table_c
   */
  auto projection_node = std::make_shared<ProjectionNode>(Expression::create_columns({ColumnID{0}}));
  auto join_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{3}), ScanType::OpGreaterThan);
  auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  auto table_a_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_node = std::make_shared<StoredTableNode>("table_b");
  auto table_c_node = std::make_shared<StoredTableNode>("table_c");
  auto predicate_a_node = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpEquals, 3);
  auto predicate_b_node = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpLessThan, ColumnID{0});
  auto predicate_c_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 4);

  predicate_a_node->set_left_child(join_node);
  predicate_b_node->set_left_child(cross_join_node);
  predicate_c_node->set_left_child(table_c_node);
  cross_join_node->set_left_child(table_b_node);
  cross_join_node->set_right_child(predicate_c_node);
  join_node->set_left_child(table_a_node);
  join_node->set_right_child(predicate_b_node);
  projection_node->set_left_child(predicate_a_node);

  // Searching from join_a should yield a non-empty join graph
  const auto join_graph = JoinGraphBuilder::build_join_graph(predicate_a_node);

  EXPECT_VERTEX_NODES(join_graph,
                      std::vector<std::shared_ptr<AbstractASTNode>>({table_a_node, table_b_node, table_c_node}));

  EXPECT_EQ(join_graph->edges().size(), 3u);
  EXPECT_JOIN_EDGE(join_graph, table_a_node, table_c_node, ColumnID{0}, ColumnID{1}, ScanType::OpGreaterThan);
  EXPECT_JOIN_EDGE(join_graph, table_b_node, table_c_node, ColumnID{0}, ColumnID{0}, ScanType::OpGreaterThan);
  EXPECT_CROSS_JOIN_EDGE(join_graph, table_b_node, table_c_node);
  EXPECT_VERTEX_PREDICATE(join_graph, table_c_node, ColumnID{0}, ScanType::OpGreaterThan, 4);
  EXPECT_VERTEX_PREDICATE(join_graph, table_b_node, ColumnID{1}, ScanType::OpEquals, 3);
}

TEST_F(JoinGraphTest, BuildJoinGraphWithCrossJoins) {
  /**
   * Test that
   *    - Cross Joins create Edges from all their left children to all their right children.
   */

  /**
   *             ___________CrossJoin_0 ______________
   *            /                                    \
   *   ___CrossJoin_1 __                   Join (table_c.a > table_b.b)
   *  /                 \                 /                           \
   * table_a            table_b_0  table_c                            table_b_1
   *
   */
  auto cross_join_0 = std::make_shared<JoinNode>(JoinMode::Cross);
  auto cross_join_1 = std::make_shared<JoinNode>(JoinMode::Cross);
  auto join_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);

  auto table_a_node = std::make_shared<StoredTableNode>("table_a");
  auto table_b_0_node = std::make_shared<StoredTableNode>("table_b");
  auto table_b_1_node = std::make_shared<StoredTableNode>("table_b");
  auto table_c_node = std::make_shared<StoredTableNode>("table_c");

  cross_join_0->set_children(cross_join_1, join_node);
  cross_join_1->set_children(table_a_node, table_b_0_node);
  join_node->set_children(table_c_node, table_b_1_node);

  // Searching from join_a should yield a non-empty join graph
  const auto join_graph = JoinGraphBuilder::build_join_graph(cross_join_0);

  EXPECT_VERTEX_NODES(join_graph, std::vector<std::shared_ptr<AbstractASTNode>>(
                                      {table_a_node, table_b_0_node, table_c_node, table_b_1_node}));

  EXPECT_EQ(join_graph->edges().size(), 3u);
  EXPECT_CROSS_JOIN_EDGE(join_graph, table_a_node, table_b_0_node);
  EXPECT_CROSS_JOIN_EDGE(join_graph, table_a_node, table_c_node);
  EXPECT_JOIN_EDGE(join_graph, table_c_node, table_b_1_node, ColumnID{0}, ColumnID{1}, ScanType::OpGreaterThan);
}

}  // namespace opossum
