#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

PredicateMergeRule::PredicateMergeRule(const size_t optimization_threshold)
    : _optimization_threshold(optimization_threshold) {}

void PredicateMergeRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateMergeRule needs root to hold onto");

  visit_lqp(root, [&](const auto& node) {
    size_t lqp_complexity = 0;
    visit_lqp(node, [&](const auto& sub_node) {
      switch (sub_node->type) {
        case LQPNodeType::Predicate:
          return LQPVisitation::VisitInputs;

        case LQPNodeType::Union:
          lqp_complexity++;
          return LQPVisitation::VisitInputs;

        default:
          return LQPVisitation::DoNotVisitInputs;
      }
    });

    // Simple heuristic: The PredicateMergeRule is more likely to improve the performance for complex LQPs with many
    // UNIONs.
    if (lqp_complexity >= _optimization_threshold) {
      _merge_subplan(node, std::nullopt);
    }

    return LQPVisitation::VisitInputs;
  });
}

/**
 * Merge an LQP that only consists of PredicateNodes and UnionNodes into a single PredicateNode. The
 * subsequent_expression parameter passes the translated expressions to the translation of its children nodes, which
 * enables to add the translated expression of child node before its parent node to the output expression.
 */
std::shared_ptr<AbstractExpression> PredicateMergeRule::_merge_subplan(
    const std::shared_ptr<AbstractLQPNode>& begin,
    const std::optional<const std::shared_ptr<AbstractExpression>>& subsequent_expression) const {
  switch (begin->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(begin);
      auto expression = predicate_node->predicate();
      // Do not concatenate expressions at the bottom of a diamond because they will be concatenated later.
      if (subsequent_expression && begin->output_count() == 1) {
        expression = and_(expression, *subsequent_expression);
      }

      const auto left_input_expression = _merge_subplan(begin->left_input(), expression);
      if (begin->left_input()->output_count() == 1 && left_input_expression) {
        // Do not merge PredicateNodes with nodes that have multiple outputs because this would unnecessarily inflate
        // the resulting logical expression. Instead, wait until the lower node has only one output. This is the case
        // after a diamond was resolved.
        lqp_remove_node(begin->left_input());
        predicate_node->node_expressions[0] = left_input_expression;
        return left_input_expression;
      } else {
        return expression;
      }
    }

    case LQPNodeType::Union: {
      const auto union_node = std::dynamic_pointer_cast<UnionNode>(begin);
      const auto left_input_expression = _merge_subplan(begin->left_input(), std::nullopt);
      const auto right_input_expression = _merge_subplan(begin->right_input(), std::nullopt);
      if (left_input_expression && right_input_expression) {
        auto expression = or_(left_input_expression, right_input_expression);

        // Do not concatenate expressions at the bottom of a diamond because they will be concatenated later.
        if (subsequent_expression && begin->output_count() == 1) {
          expression = and_(expression, *subsequent_expression);
        }

        lqp_remove_node(begin->left_input());
        lqp_remove_node(begin->right_input());
        Assert(begin->left_input() == begin->right_input(), "The LQP should be an \"empty\" diamond now.");
        begin->set_right_input(nullptr);
        const auto predicate_node = PredicateNode::make(expression);
        lqp_replace_node(begin, predicate_node);

        // The new predicate node might be mergeable with an underlying node now.
        return _merge_subplan(predicate_node, std::nullopt);
      } else {
        return nullptr;
      }
    }

    default:
      return nullptr;
  }
}

}  // namespace opossum
