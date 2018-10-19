#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class PredicateNode;

/**
 * Heuristic rule pushing non-expensive predicates down as far as possible (to reduce the result set early on) and
 * pulling expensive predicates up as far as possible.
 *
 * PredicatePlacementRule::_is_expensive_predicate() determines what constitutes "expensive". Right now, we consider
 * predicates involving a correlated subselect as "expensive" and all other predicates as non-expensive.
 */
class PredicatePlacementRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  // Traverse the LQP and perform push downs of predicates.
  // @param push_down_nodes stores nodes from higher up in the tree for which we're currently searching the lowest
  //                        possible position.
  static void _push_down_traversal(const std::shared_ptr<AbstractLQPNode>& current_node, const LQPInputSide input_side,
                                   std::vector<std::shared_ptr<PredicateNode>>& push_down_nodes);

  // Traverse the LQP and pull up expensive predicates.
  // @returns expensive predicates from the LQP below @param current_node @param input_side.
  //          The caller (i.e. _pull_up_traversal itself) has to decide which nodes are placed at the current position
  //          and which are being pushed further up, past @param current_node
  static std::vector<std::shared_ptr<PredicateNode>> _pull_up_traversal(
      const std::shared_ptr<AbstractLQPNode>& current_node, const LQPInputSide input_side);

  // Insert a set of nodes below @param node on input side @param input_side
  static void _insert_nodes(const std::shared_ptr<AbstractLQPNode>& node, const LQPInputSide input_side,
                            const std::vector<std::shared_ptr<PredicateNode>>& predicate_nodes);

  // Judge whether a predicate is expensive and should be pulled up. All non-expensive predicates get pushed down.
  static bool _is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate);
};
}  // namespace opossum
