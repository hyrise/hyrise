#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/join_detection_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

struct JoinDetectionTestParam {
  const uint32_t line;
  const std::string query;
  const uint8_t number_of_detectable_cross_joins;
};

class JoinDetectionRuleTest : public StrategyBaseTest, public ::testing::WithParamInterface<JoinDetectionTestParam> {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("c", load_table("src/test/tables/int_float.tbl"));

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

  size_t _count_cross_joins(const std::shared_ptr<AbstractLQPNode>& node) {
    auto count = size_t{0};
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
      if (join_node->join_mode == JoinMode::Cross) {
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

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Cross,
      _table_node_a,
      _table_node_b));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _table_node_a,
    _table_node_b);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _b_a),
      JoinNode::make(JoinMode::Cross,
        _table_node_a,
        _table_node_b)));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
      _table_node_a,
      _table_node_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Cross,
      _table_node_a,
      _table_node_b));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Cross,
      _table_node_a,
      _table_node_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinDetectionRuleTest, Nop) {
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
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Cross,
      _table_node_a,
      _table_node_b));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Cross,
      _table_node_a,
      _table_node_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _a_b),
      JoinNode::make(JoinMode::Cross,
        _table_node_a,
        _table_node_b)));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _a_b),
      JoinNode::make(JoinMode::Cross,
        _table_node_a,
        _table_node_b)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Inner, equals_(_a_b, _b_b),
      _table_node_a,
      _table_node_b)));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Inner, equals_(_a_b, _b_b),
      _table_node_a,
      _table_node_b)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Cross,
      JoinNode::make(JoinMode::Cross,
        _table_node_a,
        _table_node_b),
      _table_node_c)));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Cross,
      JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
        _table_node_a,
        _table_node_b),
    _table_node_c));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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
  const auto predicate_node = PredicateNode::make(equals_(_b_a, _c_b));

  predicate_node->set_left_input(join_node1);
  join_node1->set_left_input(_table_node_a);
  join_node1->set_right_input(join_node2);
  join_node2->set_left_input(_table_node_b);
  join_node2->set_right_input(_table_node_c);

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, predicate_node);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Cross,
    _table_node_a,
    JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_b),
      _table_node_b,
      _table_node_c));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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

  const auto predicate_node = PredicateNode::make(equals_(_c_a, _a_a));
  predicate_node->set_left_input(join_node2);

  const auto projection_node = ProjectionNode::make(expression_vector(_a_a));
  projection_node->set_left_input(predicate_node);

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, projection_node);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    JoinNode::make(JoinMode::Inner, equals_(_c_a, _a_a),
      JoinNode::make(JoinMode::Cross,
        _table_node_a,
        _table_node_b),
    _table_node_c));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_P(JoinDetectionRuleTest, JoinDetectionSQL) {
  JoinDetectionTestParam params = GetParam();

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(params.query, &parse_result);
  auto node = SQLTranslator{}.translate_parser_result(parse_result)[0];

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
