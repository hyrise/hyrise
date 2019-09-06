#pragma once

#include <memory>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * A rule that brings join predicates of a multi-predicate-join into an efficient order.
 * Each predicate is only evaluated on tuples that passed all previous predicates, so 
 * predicates are sorted according to their estimated cardinalities in ascending order.
 */
class JoinPredicateOrderingRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
