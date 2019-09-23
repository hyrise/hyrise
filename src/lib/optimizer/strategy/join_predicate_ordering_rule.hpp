#pragma once

#include <memory>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * A rule that brings join predicates of a multi-predicate-join into an efficient order.
 *
 * Operators like the hash join operate on the primary predicate, i.e., they hash the two columns that are compared
 * in the first predicate. Secondary predicates are evaluated using accessors, which is significantly more expensive.
 * As such, a good predicate order is even more important that it is for regular (i.e., non-join) predicates.
 * Furthermore, the hash join only supports equals predicates, so the most selective equals predicate is moved to the
 * front.
 */
class JoinPredicateOrderingRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
