#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/pqp_expression.hpp"
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

    _table_node_a = StoredTableNode::make("a");
    _table_node_b = StoredTableNode::make("b");
    _table_node_c = StoredTableNode::make("c");

    _a_a = LQPColumnReference{_table_node_a, ColumnID{0}};
    _a_b = LQPColumnReference{_table_node_a, ColumnID{1}};
    _b_a = LQPColumnReference{_table_node_b, ColumnID{0}};
    _b_b = LQPColumnReference{_table_node_b, ColumnID{1}};
    _c_a = LQPColumnReference{_table_node_c, ColumnID{1}};
    _c_b = LQPColumnReference{_table_node_c, ColumnID{1}};

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

    if (node->left_input()) {
      count += _count_cross_joins(node->left_input());
    }
    if (node->right_input()) {
      count += _count_cross_joins(node->right_input());
    }

    return count;
  }

  std::shared_ptr<StoredTableNode> _table_node_a, _table_node_b, _table_node_c;
  std::shared_ptr<JoinDetectionRule> _rule;

  LQPColumnReference _a_a, _a_b, _b_a, _b_b, _c_a, _c_b;
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
  const auto cross_join_node = JoinNode::make(JoinMode::Cross);
  cross_join_node->set_left_input(_table_node_a);
  cross_join_node->set_right_input(_table_node_b);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(cross_join_node);

  // Apply rule
  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);
  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output, PredicateCondition::Equals, _a_a, _b_a);

  ASSERT_NE(output->left_input(), nullptr);
  ASSERT_NE(output->right_input(), nullptr);
  EXPECT_EQ(output->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->right_input()->type(), LQPNodeType::StoredTable);
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
  const auto cross_join_node = JoinNode::make(JoinMode::Cross);
  cross_join_node->set_left_input(_table_node_a);
  cross_join_node->set_right_input(_table_node_b);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(cross_join_node);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_input(), PredicateCondition::Equals, _a_a, _b_a);

  EXPECT_EQ(output->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto cross_join_node = JoinNode::make(JoinMode::Cross);
  cross_join_node->set_left_input(_table_node_a);
  cross_join_node->set_right_input(_table_node_b);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(cross_join_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  ASSERT_CROSS_JOIN_NODE(output->left_input());

  EXPECT_EQ(output->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto cross_join_node = JoinNode::make(JoinMode::Cross);
  cross_join_node->set_left_input(_table_node_a);
  cross_join_node->set_right_input(_table_node_b);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _a_b);
  predicate_node->set_left_input(cross_join_node);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  EXPECT_EQ(output->left_input()->type(), LQPNodeType::Predicate);
  ASSERT_CROSS_JOIN_NODE(output->left_input()->left_input());
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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

  const auto join_node = JoinNode::make(JoinMode::Inner, std::make_pair(_a_b, _b_b), PredicateCondition::Equals);
  join_node->set_left_input(_table_node_a);
  join_node->set_right_input(_table_node_b);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(join_node);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  EXPECT_EQ(output->left_input()->type(), LQPNodeType::Predicate);
  ASSERT_INNER_JOIN_NODE(output->left_input()->left_input(), PredicateCondition::Equals, _a_b, _b_b);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto join_node1 = JoinNode::make(JoinMode::Cross);
  join_node1->set_left_input(_table_node_a);
  join_node1->set_right_input(_table_node_b);

  const auto join_node2 = JoinNode::make(JoinMode::Cross);
  join_node2->set_left_input(join_node1);
  join_node2->set_right_input(_table_node_c);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(join_node2);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);
  ASSERT_EQ(output->left_input()->type(), LQPNodeType::Join);

  const auto first_join_node = std::dynamic_pointer_cast<JoinNode>(output->left_input());
  EXPECT_EQ(first_join_node->join_mode(), JoinMode::Cross);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_input()->left_input(), PredicateCondition::Equals, _a_a, _b_a);

  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto join_node1 = JoinNode::make(JoinMode::Cross);
  const auto join_node2 = JoinNode::make(JoinMode::Cross);
  const auto predicate_node = PredicateNode::make(_b_a, PredicateCondition::Equals, _c_b);

  predicate_node->set_left_input(join_node1);
  join_node1->set_left_input(_table_node_a);
  join_node1->set_right_input(join_node2);
  join_node2->set_left_input(_table_node_b);
  join_node2->set_right_input(_table_node_c);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output, join_node1);
  EXPECT_EQ(output->left_input(), _table_node_a);
  ASSERT_INNER_JOIN_NODE(output->right_input(), PredicateCondition::Equals, _b_a, _c_b);
  EXPECT_EQ(output->right_input()->left_input(), _table_node_b);
  EXPECT_EQ(output->right_input()->right_input(), _table_node_c);
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
  const auto join_node1 = JoinNode::make(JoinMode::Cross);
  join_node1->set_left_input(_table_node_a);
  join_node1->set_right_input(_table_node_b);

  const auto join_node2 = JoinNode::make(JoinMode::Cross);
  join_node2->set_left_input(join_node1);
  join_node2->set_right_input(_table_node_c);

  const auto predicate_node = PredicateNode::make(_c_a, PredicateCondition::Equals, _a_a);
  predicate_node->set_left_input(join_node2);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(predicate_node);

  auto output = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(output->type(), LQPNodeType::Projection);

  // Verification of the new JOIN
  ASSERT_INNER_JOIN_NODE(output->left_input(), PredicateCondition::Equals, _a_a, _c_a);

  EXPECT_EQ(output->left_input()->left_input()->type(), LQPNodeType::Join);
  const auto second_join_node = std::dynamic_pointer_cast<JoinNode>(output->left_input()->left_input());
  EXPECT_EQ(second_join_node->join_mode(), JoinMode::Cross);

  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto join_node = JoinNode::make(JoinMode::Cross);
  join_node->set_left_input(_table_node_a);
  join_node->set_right_input(_table_node_b);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a),
                                                               LQPExpression::create_column(_b_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(join_node);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(projection_node);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output->type(), LQPNodeType::Predicate);
  EXPECT_EQ(output->left_input()->type(), LQPNodeType::Projection);
  ASSERT_CROSS_JOIN_NODE(output->left_input()->left_input());
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
  const auto join_node = JoinNode::make(JoinMode::Cross);
  join_node->set_left_input(_table_node_a);
  join_node->set_right_input(_table_node_b);

  const std::vector<std::shared_ptr<LQPExpression>> columns = {LQPExpression::create_column(_a_a),
                                                               LQPExpression::create_column(_b_a)};
  const auto projection_node = ProjectionNode::make(columns);
  projection_node->set_left_input(join_node);

  const auto predicate_node = PredicateNode::make(_a_a, PredicateCondition::Equals, _b_a);
  predicate_node->set_left_input(projection_node);

  auto output = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(output->type(), LQPNodeType::Predicate);
  EXPECT_EQ(output->left_input()->type(), LQPNodeType::Projection);

  ASSERT_EQ(output->left_input()->left_input()->type(), LQPNodeType::Join);

  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(output->left_input()->left_input()->left_input()->type(), LQPNodeType::StoredTable);
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
