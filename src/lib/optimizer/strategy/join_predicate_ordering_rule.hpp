#pragma once

#include <memory>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * A rule that brings predicates in a multi-predicate-join into a (supposedly) efficient order.
 */
class JoinPredicateOrderingRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
