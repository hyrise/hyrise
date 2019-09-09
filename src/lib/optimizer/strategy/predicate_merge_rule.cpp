#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

/**
 * Function creates a boolean expression from an lqp. It traverses the passed lqp from top to bottom. However, an lqp is
 * evaluated from bottom to top. This requires that the order in which the translated expressions are added to the
 * output expression is the reverse order of how the nodes are traversed. The subsequent_expression parameter passes the
 * translated expressions to the translation of its children nodes which enables to add the translated expression of
 * child node before its parent node to the output expression.
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression_impl(
    const std::shared_ptr<AbstractLQPNode>& begin,
    const std::optional<const std::shared_ptr<AbstractExpression>>& subsequent_expression) {
  switch (begin->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(begin);
      const auto predicate = predicate_node->predicate();
      // todo(jj): Add comment
      const auto expression = subsequent_expression && begin->output_count() == 1
                                  ? expression_functional::and_(predicate, *subsequent_expression)
                                  : predicate;
      const auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), expression);
      if (begin->left_input()->output_count() == 1 && left_input_expression) {
        // Do not merge predicate nodes with nodes that have multiple outputs because this would unnecessarily inflate
        // the resulting logical expression. Instead, w
        //          std::cout << *left_input_expression << std::endl;
        lqp_remove_node(begin->left_input());
        predicate_node->node_expressions[0] = left_input_expression;
        return left_input_expression;
      } else {
        return expression;
      }
    }

    case LQPNodeType::Union: {
      const auto union_node = std::dynamic_pointer_cast<UnionNode>(begin);
      const auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), std::nullopt);
      const auto right_input_expression = lqp_subplan_to_boolean_expression_impl(begin->right_input(), std::nullopt);
      if (left_input_expression && right_input_expression) {
        //          std::cout << *left_input_expression << std::endl;
        //          std::cout << *right_input_expression << std::endl;
        // todo(jj): Add comment
        auto expression = expression_functional::or_(left_input_expression, right_input_expression);
        expression = subsequent_expression && begin->output_count() == 1
                         ? expression_functional::and_(expression, *subsequent_expression)
                         : expression;

        //          std::cout << "prepare merge" << std::endl;
        lqp_remove_node(begin->left_input());
        lqp_remove_node(begin->right_input());
        const auto predicate_node = PredicateNode::make(expression);
        lqp_replace_node(begin, predicate_node);
        Assert(!predicate_node->right_input() || predicate_node->left_input() == predicate_node->right_input(),
               "The new predicate node must not have two different inputs");
        predicate_node->set_right_input(nullptr);
        //          std::cout << "merge" << std::endl;
        return lqp_subplan_to_boolean_expression_impl(
            predicate_node, std::nullopt);  // The new predicate node might be mergeable with an underlying node now
      } else {
        return nullptr;
      }
    }

    default:
      return nullptr;
  }
}

// Function wraps the call to the lqp_subplan_to_boolean_expression_impl() function to hide its third parameter,
// subsequent_predicate, which is only used internally.
std::shared_ptr<AbstractExpression> PredicateMergeRule::_lqp_subplan_to_boolean_expression(
    const std::shared_ptr<AbstractLQPNode>& begin) const {
  return lqp_subplan_to_boolean_expression_impl(begin, std::nullopt);
}

void PredicateMergeRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (!node) {
    return;
  }

  //  auto top_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  std::set<std::shared_ptr<AbstractLQPNode>> break_nodes;

  // Simple heuristic: The PredicateMergeRule is more likely to improve the performance for complex LQPs with UNIONs
  auto needsMerge = false;
  visit_lqp(node, [&](const auto& sub_node) {
    switch (sub_node->type) {
      case LQPNodeType::Predicate:
        return LQPVisitation::VisitInputs;

      case LQPNodeType::Union:
        //          top_nodes.emplace_back(sub_node);
        needsMerge = true;
        return LQPVisitation::VisitInputs;

      default:
        break_nodes.insert(sub_node);
        return LQPVisitation::DoNotVisitInputs;
    }
  });

  //  std::cout << "BBB: " << break_nodes.size() << std::endl;
  for (const auto& next_node : break_nodes) {
    apply_to(next_node->left_input());
    apply_to(next_node->right_input());
  }

  //  // _splitConjunction() and _splitDisjunction() split up logical expressions by calling each other recursively
  //  std::cout << "BBB: " << top_nodes.size() << std::endl;
  //  for (const auto& top_node : top_nodes) {
  if (needsMerge) {
    _lqp_subplan_to_boolean_expression(node);
  }
  //  if (merged_expr) {
  ////    std::cout << "AAAAA: " << *merged_expr << std::endl;
  //  }

  //    if (!_mergeConjunction(predicate_node)) {
  //      // If there is no conjunction at the top level, try to split disjunction first
  //      _mergeDisjunction(predicate_node);
  //    }
  //  }

  //  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(root)) {
  //    _mergeConjunction(predicate_node);
  //  } else if (const auto union_node = std::dynamic_pointer_cast<UnionNode>(root)) {
  //    if (union_node->union_mode == UnionMode::Positions) {
  //      _mergeDisjunction(union_node);
  //    }
  //  }
}

}  // namespace opossum
