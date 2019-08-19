#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

// TODO Doc

class InExpressionRewriteRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  enum class Algorithm {  // TODO remove or keep for tests?
    Auto,
    ExpressionEvaluator,
    Join,
    Disjunction
  };
  static inline Algorithm forced_algorithm{Algorithm::Disjunction};
};

}  // namespace opossum
