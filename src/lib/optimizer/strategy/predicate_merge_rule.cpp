#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

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

    if (lqp_complexity >= optimization_threshold) {
      _merge_subplan(node, std::nullopt);
    }

    return LQPVisitation::VisitInputs;
  });
}

/**
 * Merge a subplan that only consists of PredicateNodes and UnionNodes into a single PredicateNode. The
 * subsequent_expression parameter passes the translated expressions to the translation of its children nodes, which
 * allows to add the translated expression of child node before its parent node to the output expression.
 *
 * A subplan consists of linear "chain" and forked "diamond" parts.
 *
 * EXAMPLE:
 *         Step 1                   Step 2                   Step 3                         Step 4
 *
 *           |                        |                        |                              |
 *      ___Union___              ___Union___           Predicate (A OR B)      Predicate ((D AND C) AND (A OR B))
 *    /            \            /           \                  |                              |
 * Predicate (A)   |         Predicate (A)  |                  |
 *    |            |           |            |                  |
 *    |       Predicate (B)    |      Predicate (B)            |
 *    \           /            \          /                    |
 *     Predicate (C)          Predicate (D AND C)     Predicate (D AND C)
 *           |                        |                        |
 *     Predicate (D)
 *           |
 */
std::shared_ptr<AbstractExpression> PredicateMergeRule::_merge_subplan(
    const std::shared_ptr<AbstractLQPNode>& begin,
    const std::optional<const std::shared_ptr<AbstractExpression>>& subsequent_expression) const {
  switch (begin->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(begin);
      auto expression = predicate_node->predicate();
      if (subsequent_expression && begin->output_count() == 1) {
        // Only merge inside a chain.
        expression = and_(expression, *subsequent_expression);
      }

      const auto left_input_expression = _merge_subplan(begin->left_input(), expression);
      if (begin->left_input()->output_count() == 1 && left_input_expression) {
        // Only merge inside a chain. Nodes at the bottom of a diamond would unnecessarily inflate the resulting logical
        // expression now.
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

        if (subsequent_expression && begin->output_count() == 1) {
          // Only merge inside a chain.
          expression = and_(expression, *subsequent_expression);
        }

        lqp_remove_node(begin->left_input());
        lqp_remove_node(begin->right_input());
        Assert(begin->left_input() == begin->right_input(), "The LQP should be an \"empty\" diamond now.");
        begin->set_right_input(nullptr);
        const auto predicate_node = PredicateNode::make(expression);
        lqp_replace_node(begin, predicate_node);

        // The diamond was merged into a new PredicateNode, which might be mergeable with the underlying node now.
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
