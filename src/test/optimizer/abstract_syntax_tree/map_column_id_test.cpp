#include "gtest/gtest.h"

#include <memory>

#include "optimizer/abstract_syntax_tree/ast_utils.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/mock_table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/expression.hpp"

namespace opossum {

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

  /**
   *     ([0..2]=C, [3..4]=A)
   *            |
   *         CrossJoin
   *        /         \
   *       C           A
   */
  const auto join_b = std::make_shared<JoinNode>(JoinMode::Cross);
  join_b->set_children(_table_c, _table_a);

  const auto column_origins_b = join_b->get_column_origins();

  ASSERT_EQ(column_origins_b.size(), 5u);
  EXPECT_EQ(column_origins_b[0], ColumnOrigin(_table_c, ColumnID{0}));
  EXPECT_EQ(column_origins_b[1], ColumnOrigin(_table_c, ColumnID{1}));
  EXPECT_EQ(column_origins_b[2], ColumnOrigin(_table_c, ColumnID{2}));
  EXPECT_EQ(column_origins_b[3], ColumnOrigin(_table_a, ColumnID{0}));
  EXPECT_EQ(column_origins_b[4], ColumnOrigin(_table_a, ColumnID{1}));
}

TEST_F(MapColumnIDTest, GetColumnOriginsMultipleJoinsAndProjection) {

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

TEST_F(MapColumnIDTest, Basic) {
  /**
   * reorder _plan_a to
   *
   *    Predicate (a.1 == b.1)
   *            |
   *      ([0..3]=B, [4..5]=A)
   *            |
   *        CrossJoin
   *       /         \
   *      B           A
   */

  const auto join_plan_a = std::make_shared<JoinNode>(JoinMode::Cross);
  const auto predicate = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpEquals, ColumnID{3});

  join_plan_a->set_children(_table_a, _table_b);
  predicate->set_left_child(join_plan_a);

  const auto column_origins_a = join_plan_a->get_column_origins();

  ASSERT_EQ(column_origins_a.size(), 6u);
  EXPECT_EQ(column_origins_a[0], ColumnOrigin(_table_a, ColumnID{0}));
  EXPECT_EQ(column_origins_a[1], ColumnOrigin(_table_a, ColumnID{1}));
  EXPECT_EQ(column_origins_a[2], ColumnOrigin(_table_b, ColumnID{0}));
  EXPECT_EQ(column_origins_a[3], ColumnOrigin(_table_b, ColumnID{1}));
  EXPECT_EQ(column_origins_a[4], ColumnOrigin(_table_b, ColumnID{2}));
  EXPECT_EQ(column_origins_a[5], ColumnOrigin(_table_b, ColumnID{3}));

  const auto join_plan_b = std::make_shared<JoinNode>(JoinMode::Cross);
  join_plan_b->set_children(_table_b, _table_a);

  const auto column_origins_b = join_plan_b->get_column_origins();

  ASSERT_EQ(column_origins_b.size(), 6u);
  EXPECT_EQ(column_origins_b[0], ColumnOrigin(_table_b, ColumnID{0}));
  EXPECT_EQ(column_origins_b[1], ColumnOrigin(_table_b, ColumnID{1}));
  EXPECT_EQ(column_origins_b[2], ColumnOrigin(_table_b, ColumnID{2}));
  EXPECT_EQ(column_origins_b[3], ColumnOrigin(_table_b, ColumnID{3}));
  EXPECT_EQ(column_origins_b[4], ColumnOrigin(_table_a, ColumnID{0}));
  EXPECT_EQ(column_origins_b[5], ColumnOrigin(_table_a, ColumnID{1}));

  predicate->set_left_child(join_plan_b);
  join_plan_b->dispatch_column_id_mapping(column_origins_a);

  EXPECT_EQ(predicate->column_id(), ColumnID{5});
  EXPECT_EQ(boost::get<ColumnID>(predicate->value()), ColumnID{1});
}

}  // namespace opossum