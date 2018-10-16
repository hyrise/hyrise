#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class PredicateNode;

/**
 * Heuristic rule pushing simple predicates down as far as possible (to reduce the result set early on) and pulling
 * expensive predicates up as far as possible.
 */
class PredicatePlacementRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  // Traverse the LQP and perform push downs of predicates
  static void _push_down_traversal(const std::shared_ptr<AbstractLQPNode>& current_node,
                                  const LQPInputSide input_side,
                        std::vector<std::shared_ptr<PredicateNode>>& push_down_nodes);

  static std::vector<std::shared_ptr<PredicateNode>> _pull_up_traversal(const std::shared_ptr<AbstractLQPNode>& current_node,
                                                                        const LQPInputSide input_side);

  // Insert a set of nodes below a node
  static void _insert_nodes(const std::shared_ptr<AbstractLQPNode>& node,
                                 const LQPInputSide input_side,
                                 const std::vector<std::shared_ptr<PredicateNode>>& predicate_nodes);

  // Judge whether a predicate is expensive and should be pulled up. All non-expensive predicates get pushed down.
  static bool _is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate);
};
}  // namespace opossum
