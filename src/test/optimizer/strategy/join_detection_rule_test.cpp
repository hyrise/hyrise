#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "optimizer/strategy/join_detection_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

struct JoinDetectionTestParam {
  const uint32_t line;
  const std::string query;
  const uint8_t number_of_detectable_cross_joins;
};

class JoinDetectionRuleTest : public StrategyBaseTest, public ::testing::WithParamInterface<JoinDetectionTestParam> {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("c", load_table("src/test/tables/int_float.tbl", 2));

    _table_node_a = std::make_shared<StoredTableNode>("a");
    _table_node_b = std::make_shared<StoredTableNode>("b");
    _table_node_c = std::make_shared<StoredTableNode>("c");

    _rule = std::make_shared<JoinDetectionRule>();
  }

  uint8_t _count_cross_joins(const std::shared_ptr<AbstractLQPNode>& node) {
    uint8_t count = 0u;
    if (node->type() == LQPNodeType::Join) {
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
      if (join_node->join_mode() == JoinMode::Cross) {
        count++;
      }
    }

    if (node->left_child()) {
      count += _count_cross_joins(node->left_child());
    }
    if (node->right_child()) {
      count += _count_cross_joins(node->right_child());
    }

    return count;
  }

  std::shared_ptr<StoredTableNode> _table_node_a, _table_node_b, _table_node_c;
  std::shared_ptr<JoinDetectionRule> _rule;
};

TEST_F(JoinDetectionRuleTest, SimpleDetectionTest) {
  /**
   * Test that
   *
   *   Predicate
   *  (a.a == b.a)
   *       |
   *     Cross
   *    /     \
   *   a       b
   *
   * gets converted to
   *
   *      Join
   *  (a.a == b.a)
   *    /     \
   *   a       b
   */

  // Generate LQP
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{2});
  predicate_node->set_left_child(cross_join_node);

  // Apply rule
  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);
  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output, ScanType::Equals, ColumnID{0}, ColumnID{0});

  EXPECT_EQ(output->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, SecondDetectionTest) {
  /**
   * Test that
   *
   *   Projection
   *     (a.a)
   *       |
   *   Predicate
   *  (a.a == b.a)
   *       |
   *     Cross
   *    /     \
   *   a       b
   *
   * gets converted to
   *
   *   Projection
   *     (a.a)
   *       |
   *      Join
   *  (a.a == b.a)
   *    /     \
   *   a       b
   */

  // Generate LQP
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{2});
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_child(), ScanType::Equals, ColumnID{0}, ColumnID{0});

  EXPECT_EQ(output->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoPredicate) {
  /**
   * Test that
   *
   *   Projection
   *     (a.a)
   *       |
   *     Cross
   *    /     \
   *   a       b
   *
   * is not manipulated
   */

  // Generate LQP
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(cross_join_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  ASSERT_CROSS_JOIN_NODE(output->left_child());

  EXPECT_EQ(output->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoMatchingPredicate) {
  /**
   * Test that
   *
   *   Projection
   *     (a.a)
   *       |
   *   Predicate
   *  (a.a == a.b)
   *       |
   *     Cross
   *    /     \
   *   a       b
   *
   * isn't manipulated.
   */

  // Generate LQP
  const auto cross_join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  cross_join_node->set_left_child(_table_node_a);
  cross_join_node->set_right_child(_table_node_b);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{1});
  predicate_node->set_left_child(cross_join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), LQPNodeType::Predicate);
  ASSERT_CROSS_JOIN_NODE(output->left_child()->left_child());
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NonCrossJoin) {
  /**
   * Test that
   *
   *   Projection
   *     (a.a)
   *       |
   *   Predicate
   *  (a.a == b.a)
   *       |
   *     Join
   *  (a.b == b.b)
   *    /     \
   *   a       b
   *
   * isn't manipulated.
   */

  const auto join_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{1}, ColumnID{1}), ScanType::Equals);
  join_node->set_left_child(_table_node_a);
  join_node->set_right_child(_table_node_b);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{2});
  predicate_node->set_left_child(join_node);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  EXPECT_EQ(output->left_child()->type(), LQPNodeType::Predicate);
  ASSERT_INNER_JOIN_NODE(output->left_child()->left_child(), ScanType::Equals, ColumnID{1}, ColumnID{1});
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, MultipleJoins) {
  /**
   * Test that
   *
   *       Projection
   *         (a.a)
   *           |
   *        Predicate
   *      (a.a == b.a)
   *            |
   *          Cross
   *         /     \
   *     Cross      c
   *    /     \
   *   a       b
   *
   *   gets converted to
   *
   *       Projection
   *         (a.a)
   *            |
   *          Cross
   *         /     \
   *      Join      c
   *  (a.a == b.a)
   *    /     \
   *   a       b
   *
   */
  const auto join_node1 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node1->set_left_child(_table_node_a);
  join_node1->set_right_child(_table_node_b);

  const auto join_node2 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node2->set_left_child(join_node1);
  join_node2->set_right_child(_table_node_c);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{2});
  predicate_node->set_left_child(join_node2);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  ASSERT_EQ(output->left_child()->type(), LQPNodeType::Join);

  const auto first_join_node = std::dynamic_pointer_cast<JoinNode>(output->left_child());
  EXPECT_EQ(first_join_node->join_mode(), JoinMode::Cross);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_child()->left_child(), ScanType::Equals, ColumnID{0}, ColumnID{0});

  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, JoinInRightChild) {
  /**
   * Test that
   *
   *        Predicate
   *      (b.a == c.b)
   *            |
   *          Cross_1
   *         /     \
   *        a     Cross_2
   *             /     \
   *            b       c
   *
   *   gets converted to
   *
   *         Cross_1
   *        /     \
   *       a   InnerJoin (b.a == c.b)
   *          /                     \
   *         b                       c
   *
   */
  const auto join_node1 = std::make_shared<JoinNode>(JoinMode::Cross);
  const auto join_node2 = std::make_shared<JoinNode>(JoinMode::Cross);
  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::Equals, ColumnID{5});

  predicate_node->set_left_child(join_node1);
  join_node1->set_left_child(_table_node_a);
  join_node1->set_right_child(join_node2);
  join_node2->set_left_child(_table_node_b);
  join_node2->set_right_child(_table_node_c);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output, join_node1);
  EXPECT_EQ(output->left_child(), _table_node_a);
  ASSERT_INNER_JOIN_NODE(output->right_child(), ScanType::Equals, ColumnID{0}, ColumnID{1});
  EXPECT_EQ(output->right_child()->left_child(), _table_node_b);
  EXPECT_EQ(output->right_child()->right_child(), _table_node_c);
}

TEST_F(JoinDetectionRuleTest, MultipleJoins2) {
  /**
   * Test that
   *
   *        Projection
   *          (a.a)
   *            |
   *        Predicate
   *      (a.a == c.a)
   *            |
   *          Cross
   *         /     \
   *     Cross      c
   *    /     \
   *   a       b
   *
   *   gets converted to
   *
   *       Projection
   *         (a.a)
   *            |
   *          Join
   *       (c.a == a.a)
   *         /     \
   *      Cross     c
   *    /     \
   *   a       b
   *
   */
  const auto join_node1 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node1->set_left_child(_table_node_a);
  join_node1->set_right_child(_table_node_b);

  const auto join_node2 = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node2->set_left_child(join_node1);
  join_node2->set_right_child(_table_node_c);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{4}, ScanType::Equals, ColumnID{0});
  predicate_node->set_left_child(join_node2);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_child(), ScanType::Equals, ColumnID{0}, ColumnID{0});

  EXPECT_EQ(output->left_child()->left_child()->type(), LQPNodeType::Join);
  const auto second_join_node = std::dynamic_pointer_cast<JoinNode>(output->left_child()->left_child());
  EXPECT_EQ(second_join_node->join_mode(), JoinMode::Cross);

  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoOptimizationAcrossProjection) {
  /**
   * Test that
   *
   *        Predicate
   *      (a.a == b.a)
   *           |
   *       Projection
   *       (a.a, b.a)
   *           |
   *          Cross
   *         /     \
   *        a       b
   *
   * isn't manipulated.
   *
   * (This would be Predicate Pushdown and will be covered by a different Optimizer Rule in the future)
   *
   */
  const auto join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node->set_left_child(_table_node_a);
  join_node->set_right_child(_table_node_b);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0}),
                                                            Expression::create_column(ColumnID{2})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(join_node);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{1});
  predicate_node->set_left_child(projection_node);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output->type(), LQPNodeType::Predicate);
  EXPECT_EQ(output->left_child()->type(), LQPNodeType::Projection);
  ASSERT_CROSS_JOIN_NODE(output->left_child()->left_child());
  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(JoinDetectionRuleTest, NoJoinDetectionAcrossProjections) {
  /**
   * Test that
   *
   *        Predicate
   *      (a.a == b.a)
   *           |
   *       Projection
   *       (a.a, b.a)
   *           |
   *          Cross
   *         /     \
   *        a       b
   *
   * isn't manipulated.
   *
   * (This would be Predicate Pushdown and will be covered by a different Optimizer Rule in the future)
   *
   */
  const auto join_node = std::make_shared<JoinNode>(JoinMode::Cross);
  join_node->set_left_child(_table_node_a);
  join_node->set_right_child(_table_node_b);

  const std::vector<std::shared_ptr<Expression>> columns = {Expression::create_column(ColumnID{0}),
                                                            Expression::create_column(ColumnID{2})};
  const auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(join_node);

  const auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, ColumnID{1});
  predicate_node->set_left_child(projection_node);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output->type(), LQPNodeType::Predicate);
  EXPECT_EQ(output->left_child()->type(), LQPNodeType::Projection);

  ASSERT_EQ(output->left_child()->left_child()->type(), LQPNodeType::Join);

  EXPECT_EQ(output->left_child()->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_child()->left_child()->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_P(JoinDetectionRuleTest, JoinDetectionSQL) {
  JoinDetectionTestParam params = GetParam();

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(params.query, &parse_result);
  auto node = SQLTranslator{false}.translate_parse_result(parse_result)[0];

  auto before = _count_cross_joins(node);
  auto output = StrategyBaseTest::apply_rule(_rule, node);
  auto after = _count_cross_joins(output);

  EXPECT_EQ(before - after, params.number_of_detectable_cross_joins);
}

const JoinDetectionTestParam test_queries[] = {{__LINE__, "SELECT * FROM a, b WHERE a.a = b.a", 1},
                                               {__LINE__, "SELECT * FROM a, b, c WHERE a.a = c.a", 1},
                                               {__LINE__, "SELECT * FROM a, b, c WHERE b.a = c.a", 1}};

auto formatter = [](const testing::TestParamInfo<struct JoinDetectionTestParam> info) {
  return std::to_string(info.param.line);
};
INSTANTIATE_TEST_CASE_P(test_queries, JoinDetectionRuleTest, ::testing::ValuesIn(test_queries), formatter);

}  // namespace opossum
