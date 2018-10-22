#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/logical_expression_reducer_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

namespace {
// Exposes the protected methods of LogicalExpressionReducerRule
class Rule : public LogicalExpressionReducerRule {
 public:
  using LogicalExpressionReducerRule::_apply_to_expressions;
  using LogicalExpressionReducerRule::_collect_chained_logical_expressions;
  using LogicalExpressionReducerRule::_remove_expressions_from_chain;
  using LogicalExpressionReducerRule::MapType;
};
}  // namespace

class LogicalExpressionReducerRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("dummy", load_table("src/test/tables/five_ints_empty.tbl", Chunk::MAX_SIZE));
    _table = std::make_shared<StoredTableNode>("dummy");
    _a = equals_(LQPColumnReference(_table, ColumnID{0}), 0);
    _b = equals_(LQPColumnReference(_table, ColumnID{1}), 1);
    _c = equals_(LQPColumnReference(_table, ColumnID{2}), 2);
    _d = equals_(LQPColumnReference(_table, ColumnID{3}), 3);
    _e = equals_(LQPColumnReference(_table, ColumnID{4}), 4);
  }

  std::shared_ptr<AbstractLQPNode> _table;
  std::shared_ptr<AbstractExpression> _a, _b, _c, _d, _e;
};

TEST_F(LogicalExpressionReducerRuleTest, CollectChainedAndExpressions) {
  // a = 0 AND b = 1 AND (c = 2 OR d = 3)
  auto or_expression = or_(_c, _d);
  auto expression = and_(_a, and_(_b, and_(or_expression, _e)));

  {
    ExpressionUnorderedSet chain;
    Rule::_collect_chained_logical_expressions(expression, LogicalOperator::And, chain);
    EXPECT_EQ(chain, (ExpressionUnorderedSet{_a, _b, or_expression, _e}));
  }

  {
    ExpressionUnorderedSet chain;
    Rule::_collect_chained_logical_expressions(expression, LogicalOperator::Or, chain);
    EXPECT_EQ(chain, (ExpressionUnorderedSet{}));
  }
}

TEST_F(LogicalExpressionReducerRuleTest, CollectChainedOrExpressions) {
  // a = 0 OR b = 1 OR (c = 2 AND d = 3)
  auto and_expression = and_(_c, _d);
  auto expression = or_(_a, or_(_b, or_(and_expression, _e)));

  {
    ExpressionUnorderedSet chain;
    Rule::_collect_chained_logical_expressions(expression, LogicalOperator::Or, chain);
    EXPECT_EQ(chain, (ExpressionUnorderedSet{_a, _b, and_expression, _e}));
  }

  {
    ExpressionUnorderedSet chain;
    Rule::_collect_chained_logical_expressions(expression, LogicalOperator::And, chain);
    EXPECT_EQ(chain, (ExpressionUnorderedSet{}));
  }
}

TEST_F(LogicalExpressionReducerRuleTest, RemoveFromChainedAndExpressions) {
  // a = 0 AND b = 1 AND (c = 2 OR d = 3)
  auto or_expression = or_(_c, _d);
  auto expression = and_(_a, and_(_b, and_(or_expression, _e)));

  {
    // Remove from AND-chain
    auto expression_copy = expression->deep_copy();
    Rule::_remove_expressions_from_chain(expression_copy, LogicalOperator::And,
                                         ExpressionUnorderedSet{_a, or_expression});
    EXPECT_EQ(*expression_copy, *and_(_b, _e));
  }

  {
    // No OR-chain in this example
    auto expression_copy = expression->deep_copy();
    Rule::_remove_expressions_from_chain(expression_copy, LogicalOperator::Or,
                                         ExpressionUnorderedSet{_a, or_expression});
    EXPECT_EQ(*expression_copy, *expression);
  }
}

TEST_F(LogicalExpressionReducerRuleTest, RemoveFromChainedOrExpressions) {
  // a = 0 OR b = 1 OR (c = 2 AND d = 3)
  auto and_expression = and_(_c, _d);
  auto expression = or_(_a, or_(_b, or_(and_expression, _e)));

  {
    // Remove from OR-chain
    auto expression_copy = expression->deep_copy();
    Rule::_remove_expressions_from_chain(expression_copy, LogicalOperator::Or,
                                         ExpressionUnorderedSet{_a, and_expression});
    EXPECT_EQ(*expression_copy, *or_(_b, _e));
  }

  {
    // No AND-chain in this example
    auto expression_copy = expression->deep_copy();
    Rule::_remove_expressions_from_chain(expression_copy, LogicalOperator::And,
                                         ExpressionUnorderedSet{_a, and_expression});
    EXPECT_EQ(*expression_copy, *expression);
  }
}

TEST_F(LogicalExpressionReducerRuleTest, ApplyToOrExpression) {
  // (a AND b) OR (a AND c) -> a AND (c OR b)
  auto a_and_b = and_(_a, _b);
  auto a_and_c = and_(_a, _c);
  auto expression = or_(a_and_b, a_and_c);

  Rule::MapType previously_reduced_expressions;
  std::vector<std::shared_ptr<AbstractExpression>> expressions{expression};
  Rule{}._apply_to_expressions(expressions, previously_reduced_expressions);

  EXPECT_EQ(*expressions[0], *and_(_a, or_(_c, _b)));
}

TEST_F(LogicalExpressionReducerRuleTest, ApplyToAndExpression) {
  // (a OR b) AND (a or c) - no rewrite is expected
  auto a_or_b = and_(_a, _b);
  auto a_or_c = and_(_a, _c);
  auto expression = and_(a_or_b, a_or_c);

  Rule::MapType previously_reduced_expressions;
  std::vector<std::shared_ptr<AbstractExpression>> expressions{expression};
  Rule{}._apply_to_expressions(expressions, previously_reduced_expressions);

  EXPECT_EQ(*expressions[0], *expression);
}

TEST_F(LogicalExpressionReducerRuleTest, ApplyToProjection) {
  // (a AND b) OR (a AND c) -> a AND (c OR b)
  auto a_and_b = and_(_a, _b);
  auto a_and_c = and_(_a, _c);
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  expressions.emplace_back(or_(a_and_b, a_and_c));
  expressions.emplace_back(and_(a_and_b, a_and_c));

  auto projection = ProjectionNode::make(expressions);
  Rule{}.apply_to(projection);

  EXPECT_EQ(*projection->column_expressions()[0], *and_(_a, or_(_c, _b)));
  EXPECT_EQ(*projection->column_expressions()[1], *and_(a_and_b, a_and_c));
}

TEST_F(LogicalExpressionReducerRuleTest, ApplyToPredicate) {
  // (a AND b) OR (a AND c) -> PredicateNode{a} -> PredicateNode{b OR c}
  auto a_and_b = and_(_a, _b);
  auto a_and_c = and_(_a, _c);
  auto expression = or_(a_and_b, a_and_c);

  auto plan = LogicalPlanRootNode::make(PredicateNode::make(expression, _table));
  Rule{}.apply_to(plan);

  EXPECT_LQP_EQ(plan, LogicalPlanRootNode::make(PredicateNode::make(_a, PredicateNode::make(or_(_c, _b), _table))));
}

}  // namespace opossum
