#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

// TODO Doc

class InExpressionRewriteRule : public AbstractRule {
 public:
  // With the auto strategy, IN expressions with up to MAX_ELEMENTS_FOR_DISJUNCTION on the right side are rewritten
  // into disjunctive predicates. This value was chosen conservatively, also to keep the LQPs easy to read.
  constexpr static auto MAX_ELEMENTS_FOR_DISJUNCTION = 3;

  // With the auto strategy, IN expressions with more than MIN_ELEMENTS_FOR_JOIN are rewritten into semi joins.
  constexpr static auto MIN_ELEMENTS_FOR_JOIN = 20;

  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  enum class Strategy {
    Auto,
    ExpressionEvaluator,
    Join,
    Disjunction
  };
  static inline Strategy strategy{Strategy::Disjunction};
};

}  // namespace opossum
