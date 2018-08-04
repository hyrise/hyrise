#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes never-referenced columns from the LQP by inserting ProjectionNodes that select only those expressions needed
 * "further up" in the plan.
 */
class ColumnPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  static ExpressionUnorderedSet _collect_consumed_columns(const std::shared_ptr<AbstractLQPNode>& lqp);
  static bool _search_for_leafs_and_prune_columns(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                  const ExpressionUnorderedSet& referenced_columns);
  static void _search_for_projections_and_prune_columns(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                        const ExpressionUnorderedSet& referenced_columns);
};

}  // namespace opossum
