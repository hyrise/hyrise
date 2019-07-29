#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * This optimizer rule eliminates duplicates of the LQP by using sub-plan memoization:
 * This process uses depth-first traversal and starts integrating sub-plans into the
 * memoization structure at the leaves of the overall LQP and works upwards to the root.
 */

class DuplicateEliminationRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  void _eliminate_sub_plan_duplicates_traversal(const std::shared_ptr<AbstractLQPNode>& node) const;
  mutable std::vector<std::shared_ptr<AbstractLQPNode>> _sub_plans;
  mutable std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>>
      _original_replacement_pairs;
};

}  // namespace opossum
