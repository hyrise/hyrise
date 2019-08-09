#include "predicate_split_up_rule.hpp"

#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string PredicateSplitUpRule::name() const { return "PredicateSplitUp"; }

void PredicateSplitUpRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateSplitUpRule needs root to hold onto");

  /**
   * Step 1:
   *    - Collect PredicateNodes that can be split up into multiple ones into `predicate_nodes_to_flat_conjunctions`
   */
  auto predicate_nodes_to_flat_conjunctions =
      std::vector<std::pair<std::shared_ptr<PredicateNode>, std::vector<std::shared_ptr<AbstractExpression>>>>{};

  visit_lqp(root, [&](const auto& sub_node) {
    if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(sub_node)) {
      const auto flat_conjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);

      if (flat_conjunction.size() > 1) {
        predicate_nodes_to_flat_conjunctions.emplace_back(predicate_node, flat_conjunction);
      }
    }

    return LQPVisitation::VisitInputs;
  });

  /**
   * Step 2:
   *    - Split up qualifying PredicateNodes into multiple consecutive PredicateNodes. We have to do this in a
   *      second pass, because manipulating the LQP within `visit_lqp()`, while theoretically possible, is prone to
   *      bugs.
   */
  for (const auto& [predicate_node, flat_conjunction] : predicate_nodes_to_flat_conjunctions) {
    for (const auto& predicate_expression : flat_conjunction) {
      lqp_insert_node(predicate_node, LQPInputSide::Left, PredicateNode::make(predicate_expression));
    }
    lqp_remove_node(predicate_node);
  }
}

}  // namespace opossum
