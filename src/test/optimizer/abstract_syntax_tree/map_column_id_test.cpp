#include "gtest/gtest.h"

#include <memory>

#include "optimizer/abstract_syntax_tree/ast_utils.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/mock_table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/expression.hpp"

namespace opossum {

/**
 * Test ColumnID mapping functionality of ASTs, built around the following functions:
 *   - AbstractASTNode::get_column_origin()
 *   - AbstractASTNode::get_column_origins()
 *   - AbstractASTNode::dispatch_column_id_mapping()
 *   - AbstractASTNode::map_column_id()
 *   - ast_generate_column_id_mapping()
 */
class MapColumnIDTest : public ::testing::Test {
 public:
  void SetUp() override {
    _table_a = std::make_shared<MockTableNode>("a", 2);
    _table_b = std::make_shared<MockTableNode>("b", 4);
    _table_c = std::make_shared<MockTableNode>("c", 3);
    _table_d = std::make_shared<MockTableNode>("d", 2);
    _table_e = std::make_shared<MockTableNode>("e", 1);
  }

 protected:
  std::shared_ptr<MockTableNode> _table_a;
  std::shared_ptr<MockTableNode> _table_b;
  std::shared_ptr<MockTableNode> _table_c;
  std::shared_ptr<MockTableNode> _table_d;
  std::shared_ptr<MockTableNode> _table_e;
  std::shared_ptr<AbstractASTNode> _plan_a;
};

TEST_F(MapColumnIDTest, GetColumnOriginsBasics) {
  /**
   * Test that AbstractASTNode::get_column_origin()/get_column_origins() works in the most basic ASTs
   */

  /**
   *     ([0..1]=A, [2..4]=C)
   *            |
   *         CrossJoin
   *        /         \
   *       A           C
   */
  auto join_a = std::make_shared<JoinNode>(JoinMode::Cross);
  join_a->set_children(_table_a, _table_c);

  const auto column_origins_a = join_a->get_column_origins();

  ASSERT_EQ(column_origins_a.size(), 5u);
  EXPECT_EQ(column_origins_a[0], ColumnOrigin(_table_a, ColumnID{0}));
  EXPECT_EQ(column_origins_a[1], ColumnOrigin(_table_a, ColumnID{1}));
  EXPECT_EQ(column_origins_a[2], ColumnOrigin(_table_c, ColumnID{0}));
  EXPECT_EQ(column_origins_a[3], ColumnOrigin(_table_c, ColumnID{1}));
  EXPECT_EQ(column_origins_a[4], ColumnOrigin(_table_c, ColumnID{2}));
}

TEST_F(MapColumnIDTest, GetColumnOriginsMultipleJoinsAndProjection) {
  /**
   * Test AbstractASTNode::get_column_origins() works on more complex trees and with nodes (in this case a Projection)
   * that change the Column order.
   */

  /**
   *  Projection(A.1, C.2, C.0, E.0, D.1)
   *            |
   *  ([0..1]=A, [2..4]=C, [5..6]=D, [7]=E)
   *            |
   *         CrossJoin2
   *        /         \
   *    CrossJoin0   CrossJoin1
   *    /      \    /       \
   *   A       C   D        E
   */

  auto join_0 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_0->set_children(_table_a, _table_c);
  auto join_1 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_1->set_children(_table_d, _table_e);
  auto join_2 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_2->set_children(join_0, join_1);
  auto projection = std::make_shared<ProjectionNode>(Expression::create_columns({ColumnID{1}, ColumnID{4}, ColumnID{2}, ColumnID{7}, ColumnID{6}}));
  projection->set_left_child(join_2);

  const auto projection_column_origins = projection->get_column_origins();

  ASSERT_EQ(projection_column_origins.size(), 5u);
  EXPECT_EQ(projection_column_origins[0], ColumnOrigin(_table_a, ColumnID{1}));
  EXPECT_EQ(projection_column_origins[1], ColumnOrigin(_table_c, ColumnID{2}));
  EXPECT_EQ(projection_column_origins[2], ColumnOrigin(_table_c, ColumnID{0}));
  EXPECT_EQ(projection_column_origins[3], ColumnOrigin(_table_e, ColumnID{0}));
  EXPECT_EQ(projection_column_origins[4], ColumnOrigin(_table_d, ColumnID{1}));
}

TEST_F(MapColumnIDTest, ColumnOrderChangeBasics) {
  /**
   * Build a faked case of a join-subtree changing the order in which it concatenates tables.
   * This tests dispatch_column_id_mapping() and, indirectly, ast_generate_column_id_mapping()
   */

  /**
   * Plan before:
   *
   *    Predicate (a.1 == b.1)
   *            |
   *      ([0..1]=A, [2..5]=B)
   *            |
   *   CrossJoin(=join_plan_a)
   *       /         \
   *      A           B
   */

  const auto join_plan_a = std::make_shared<JoinNode>(JoinMode::Cross);
  const auto predicate = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpEquals, ColumnID{3});

  join_plan_a->set_children(_table_a, _table_b);
  predicate->set_left_child(join_plan_a);

  const auto column_origins_a = join_plan_a->get_column_origins();

  /**
   * Removing the old join plan is not strictly necessary, but is what would happen irl
   */
  _table_a->remove_from_tree();
  _table_b->remove_from_tree();
  join_plan_a->remove_from_tree();

  /**
   * Plan after reordering:
   *
   *    Predicate (a.1 == b.1)
   *            |
   *      ([0..3]=B, [4..5]=A)
   *            |
   *    CrossJoin(=join_plan_a)
   *       /         \
   *      B           A
   */
  const auto join_plan_b = std::make_shared<JoinNode>(JoinMode::Cross);

  join_plan_b->set_children(_table_b, _table_a);
  predicate->set_left_child(join_plan_b);

  join_plan_b->dispatch_column_id_mapping(column_origins_a);

  EXPECT_EQ(predicate->column_id(), ColumnID{5});
  EXPECT_EQ(boost::get<ColumnID>(predicate->value()), ColumnID{1});
}

TEST_F(MapColumnIDTest, ColumnOrderChangeComplex) {
  /**
   * Just the same as ColumnOrderChangeBasics, but with a more complex (taken with a grain of salt compared to real
   * world scenarios) AST
   */

  /**
   * Plan before:
   *
   *          Predicate(a.1 = d.0)
   *                   |
   *      Projection(d.1, d.0, a.1, c.1, b.3)
   *                   |
   *   ([0..1]=A, [2..5]=B, [6..8]=C, [9..10]=D)
   *                   |
   *        CrossJoin(=join_plan_a)
   *        /                     \
   *    CrossJoin_0             CrossJoin_1
   *    /       \               /       \
   *   A        B              C         D
   */
  auto predicate = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpEquals, ColumnID{1});
  auto projection = std::make_shared<ProjectionNode>(Expression::create_columns({ColumnID{10}, ColumnID{9}, ColumnID{1}, ColumnID{7}, ColumnID{5}}));
  auto join_plan_a = std::make_shared<JoinNode>(JoinMode::Cross);
  auto join_0 = std::make_shared<JoinNode>(JoinMode::Cross);
  auto join_1 = std::make_shared<JoinNode>(JoinMode::Cross);

  join_0->set_children(_table_a, _table_b);
  join_1->set_children(_table_c, _table_d);
  join_plan_a->set_children(join_0, join_1);
  projection->set_left_child(join_plan_a);
  predicate->set_left_child(projection);

  auto column_origins_a = join_plan_a->get_column_origins();

  /**
   * Removing the old join plan is not strictly necessary, but is what would happen irl
   */
  _table_a->remove_from_tree();
  _table_b->remove_from_tree();
  _table_c->remove_from_tree();
  _table_d->remove_from_tree();
  join_0->remove_from_tree();
  join_1->remove_from_tree();
  join_plan_a->remove_from_tree();

  /**
   * Plan after:
   *
   *          Predicate(a.1 = d.0)
   *                   |
   *      Projection(d.1, d.0, a.1, c.1, b.3)
   *                   |
   *   ([0..1]=D, [2..4]=C, [5..6]=A, [7..10]=B)
   *                   |
   *      CrossJoin(=join_plan_b)
   *        /         \
   *    CrossJoin_2    B
   *    /       \
   *   D        CrossJoin_3
   *            /       \
   *           C        A
   *
   */
  auto join_plan_b = std::make_shared<JoinNode>(JoinMode::Cross);
  auto join_2 = std::make_shared<JoinNode>(JoinMode::Cross);
  auto join_3 = std::make_shared<JoinNode>(JoinMode::Cross);

  join_3->set_children(_table_c, _table_a);
  join_2->set_children(_table_d, join_3);
  join_plan_b->set_children(join_2, _table_b);

  projection->set_left_child(join_plan_b);
  join_plan_b->dispatch_column_id_mapping(column_origins_a);

  /**
   * Test that the ColumnIDs in the Projection were adapted.
   */
  EXPECT_EQ(projection->column_expressions().at(0)->column_id(), ColumnID{1});
  EXPECT_EQ(projection->column_expressions().at(1)->column_id(), ColumnID{0});
  EXPECT_EQ(projection->column_expressions().at(2)->column_id(), ColumnID{6});
  EXPECT_EQ(projection->column_expressions().at(3)->column_id(), ColumnID{3});
  EXPECT_EQ(projection->column_expressions().at(4)->column_id(), ColumnID{10});
}

}  // namespace opossum