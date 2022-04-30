#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

// Depending on the size and type of IN expression `a IN (...)`, and also its input's size, this rule rewrites the
// expression to
// a) a bunch of disjunctive predicate and union nodes (equivalent to `a = 1 OR a = 2`) if the elements are of the same
//    type, if the `IN` is not part of a `FunctionExpression`, and if one of the following conditions is fulfilled:
//     - Either the right side has fewer elements than MIN_ELEMENTS_FOR_EXPRESSION_EVALUATOR,
//     - or the input node's cardinality is larger than MAX_ROWS_FOR_EXPRESSION_EVALUATOR.
// b) a semi/anti join (with the list of IN values being stored in a temporary table) if the right side has more than
//    MIN_ELEMENTS_FOR_JOIN elements and the elements are of the same type. The exact value of MIN_ELEMENTS_FOR_JOIN
//    also depends on the size of the input data (see #1817). Once this becomes relevant, we might want to add a cost
//    estimator.
// Otherwise, the IN expression is untouched and will be handled by the ExpressionEvaluator.

class InExpressionRewriteRule : public AbstractRule {
 public:
  std::string name() const override;

  // With the auto strategy, IN expressions with less than MIN_ELEMENTS_FOR_EXPRESSION_EVALUATOR on the right side are
  // rewritten into disjunctive predicates.
  constexpr static auto MIN_ELEMENTS_FOR_EXPRESSION_EVALUATOR = 4;

  // With the auto strategy, IN expressions with MIN_ELEMENTS_FOR_JOIN or more are rewritten into semi joins.
  constexpr static auto MIN_ELEMENTS_FOR_JOIN = 20;

  // With the auto strategy, IN expressions whose input has more than MAX_ROWS_FOR_EXPRESSION_EVALUATOR are rewritten
  // into disjunctive predicates.
  constexpr static auto MAX_ROWS_FOR_EXPRESSION_EVALUATOR = 25'000.f;

  // When checking for input cardinalities, we trust row counts of MIN_ROWS_FOR_EXPRESSION_EVALUATOR and higher only.
  // For lower row counts, the chances are high that our cardinality estimation is pretty far off. Especially in the JOB
  // benchmark, we observe row count estimates of < 1.0f for upper parts of many LQPs, where in reality row counts are
  // much higher (from a hundred thousands to millions).
  constexpr static auto MIN_ROWS_FOR_EXPRESSION_EVALUATOR = 1.f;

  // Instead of using the automatic behavior described above, the three strategies may be chosen explicitly, too. This
  // is helpful for testing and benchmarks. Note that it does not circumvent the restrictions on the element type.
  enum class Strategy { Auto, ExpressionEvaluator, Join, Disjunction };
  Strategy strategy{Strategy::Auto};

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator() const;

  mutable std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator_internal;
};

}  // namespace opossum
