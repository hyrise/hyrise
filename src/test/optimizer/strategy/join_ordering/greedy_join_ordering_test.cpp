#include "join_reordering_base_test.hpp"

#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/strategy/join_ordering/greedy_join_ordering.hpp"
#include "testing_assert.hpp"

namespace opossum {

class GreedyJoinOrderingTest : public JoinReorderingBaseTest {
};

TEST_F(GreedyJoinOrderingTest, BasicChainGraph) {
  /**
   * The chain equi-JoinGraph {C, D, E} should result in this JoinPlan:
   *
   *         ___Join (D.a == E.a)___
   *        /                       \
   *   ___Join (D.a == C.a)___       E
   *  /                       \
   * D                        C
   *
   * Reasoning: D is the smallest table and has the same overlapping range with C and E, but C is way smaller than E,
   * that is why C is added secondly and E afterwards
   */

  auto plan = GreedyJoinOrdering(_join_graph_cde_chain).run();

  ASSERT_INNER_JOIN_NODE(plan, ScanType::OpEquals, ColumnID{0}, ColumnID{0});
  ASSERT_INNER_JOIN_NODE(plan->left_child(), ScanType::OpEquals, ColumnID{0}, ColumnID{0});
  ASSERT_EQ(plan->right_child(), _table_node_e);
  ASSERT_EQ(plan->left_child()->left_child(), _table_node_d);
  ASSERT_EQ(plan->left_child()->right_child(), _table_node_c);
}

TEST_F(GreedyJoinOrderingTest, BasicCliqueGraph) {
  /**
   * The clique equi-JoinGraph {B, C, D} should result in this JoinPlan:
   *
   *        Predicate((C.a or D.a) == B.a)
   *                      |
   *         ___Join ((C.a or D.a) == B.a)___
   *        /                                \
   *   ___Join (D.a == C.a)___                B
   *  /                       \
   * D                        C
   *
   * Reasoning: D is the smallest table and has the same overlapping range with C and E, but C is way smaller than E,
   * that is why it is added secondly and B afterwards.
   * For merging B there will be 2 candidate edges whose predicates both need to be fulfilled. In order not to rely too
   * much on details of the cost model, we're not testing which edge is fulfilled by the Predicate and which by the
   * Join, but just that both are fulfilled
   */

  auto plan = GreedyJoinOrdering(_join_graph_bcd_clique).run();

  /**
   * Assert the JoinPlan structure: Joins/Predicates
   */
  ASSERT_EQ(plan->type(), ASTNodeType::Predicate);
  ASSERT_EQ(plan->left_child()->type(), ASTNodeType::Join);
  ASSERT_EQ(plan->left_child()->left_child()->type(), ASTNodeType::Join);

  /**
   * Assert leafs/vertices
   */
  ASSERT_EQ(plan->left_child()->right_child(), _table_node_b);
  ASSERT_EQ(plan->left_child()->left_child()->left_child(), _table_node_d);
  ASSERT_EQ(plan->left_child()->left_child()->right_child(), _table_node_c);

  /**
   * Assert edges
   */
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_b, _table_node_c, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_c, _table_node_d, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_b, _table_node_d, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
}

TEST_F(GreedyJoinOrderingTest, MediumSizeGraph) {
  /**
   * Expected Result:
   *
   *                    Predicate
   *                        |
   *                     _Join_
   *                    /      \
   *               Predicate    E
   *                  |
   *               _Join_
   *              /      \
   *         _Join_       C
   *        /      \
   *   _Join_       D
   *  /      \
   * A        B
   *
   * Reasoning: A is the smallest table, so we'll start with that one. A is only connected to B, so it is connected
   * next. Out of C, D and E, the join with D is the cheapest, because its a small table. Next comes C and finally E,
   * which is a large table.
   * After joining C, we can apply another predicate (either the BC or the CD edge) and after joining E we can add
   * either the BE or the DE edge as another predicate.
   */

  auto plan = GreedyJoinOrdering(_join_graph_abcde).run();

  /**
   * Assert the join graph structure: Joins/Predicates
   */
  ASSERT_EQ(plan->type(), ASTNodeType::Predicate);
  ASSERT_EQ(plan->left_child()->type(), ASTNodeType::Join);
  ASSERT_EQ(plan->left_child()->left_child()->type(), ASTNodeType::Predicate);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->type(), ASTNodeType::Join);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->left_child()->type(), ASTNodeType::Join);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->left_child()->left_child()->type(), ASTNodeType::Join);

  /**
   * Assert the join graphs vertices: Leafs
   */
  ASSERT_EQ(plan->left_child()->right_child(), _table_node_e);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->right_child(), _table_node_c);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->left_child()->right_child(), _table_node_d);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(), _table_node_a);
  ASSERT_EQ(plan->left_child()->left_child()->left_child()->left_child()->left_child()->right_child(), _table_node_b);

  /**
   * Assert all edges in the JoinGraph still exist in the JoinPlan
   */
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_a, _table_node_b, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_b, _table_node_c, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_c, _table_node_d, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_d, _table_node_e, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_b, _table_node_d, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
  EXPECT_AST_CONTAINS_JOIN_EDGE(plan, _table_node_b, _table_node_e, ColumnID{0}, ColumnID{0}, ScanType::OpEquals);
}
}