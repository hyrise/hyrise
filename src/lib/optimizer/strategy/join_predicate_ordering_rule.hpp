#pragma once

#include <memory>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractCostEstimator;

/**
 * A rule that brings predicates in a multi-predicate-join into a (supposedly) efficient order.
 */
class JoinPredicateOrderingRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
};

}  // namespace opossum
