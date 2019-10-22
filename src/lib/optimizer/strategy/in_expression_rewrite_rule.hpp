#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

// Depending on the size and type of an IN expression `a IN (...)`, this rule rewrites the expression to
// - a bunch of disjunctive predicate and union nodes (equivalent to `a = 1 OR a = 2`) if the right side has up to
//   MAX_ELEMENTS_FOR_DISJUNCTION elements and the elements are of the same type.
// - a semi/anti join (with the list of IN values being stored in a temporary table) if the right side has more than
//   MIN_ELEMENTS_FOR_JOIN elements and the elements are of the same type. The exact value of MIN_ELEMENTS_FOR_JOIN
//   also depends on the size of the input data (see #1817). Once this becomes relevant, we might want to add a cost
//   estimator.
// Otherwise, the IN expression is untouched and will be handled by the ExpressionEvaluator.

class InExpressionRewriteRule : public AbstractRule {
 public:
  // With the auto strategy, IN expressions with up to MAX_ELEMENTS_FOR_DISJUNCTION on the right side are rewritten
  // into disjunctive predicates. This value was chosen conservatively, also to keep the LQPs easy to read.
  constexpr static auto MAX_ELEMENTS_FOR_DISJUNCTION = 3;

  // With the auto strategy, IN expressions with MIN_ELEMENTS_FOR_JOIN or more are rewritten into semi joins.
  constexpr static auto MIN_ELEMENTS_FOR_JOIN = 20;

  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  // Instead of using the automatic behavior described above, the three strategies may be chosen explicitly, too. This
  // is helpful for testing and benchmarks. Note that it does not circumvent the restrictions on the element type.
  enum class Strategy { Auto, ExpressionEvaluator, Join, Disjunction };
  Strategy strategy{Strategy::Auto};
};

}  // namespace opossum
