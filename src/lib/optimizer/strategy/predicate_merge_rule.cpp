#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

//bool PredicateMergeRule::_mergeConjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
//  const auto flat_conjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);
//  if (flat_conjunction.size() <= 1) {
//    return false;
//  }
//
//  /**
//   * Split up PredicateNode with conjunctive chain (e.g., `PredicateNode(a AND b AND c)`) as its scan expression into
//   * multiple consecutive PredicateNodes (e.g. `PredicateNode(c) -> PredicateNode(b) -> PredicateNode(a)`).
//   */
//  for (const auto& predicate_expression : flat_conjunction) {
//    const auto& new_predicate_node = PredicateNode::make(predicate_expression);
//    lqp_insert_node(predicate_node, LQPInputSide::Left, new_predicate_node);
//    _mergeDisjunction(new_predicate_node);
//  }
//  lqp_remove_node(predicate_node);

//  if (const auto child_predicate = std::dynamic_pointer_cast<PredicateNode>(predicate_node->left_input())) {
//    if (child_predicate->output_count() == 1) {
//      and_()
//      predicate_node->node_expressions[0] = and_(predicate_node->predicate(), child_predicate->predicate());
//      lqp_remove_node(child_predicate);
//    }
//  }
//
//  return true;
//}

//void PredicateMergeRule::_mergeDisjunction(const std::shared_ptr<UnionNode>& union_node) const {
//  if (!_split_disjunction) {
//    return;
//  }
//
//  const auto flat_disjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::Or);
//  if (flat_disjunction.size() <= 1) {
//    return;
//  }
//
//  /**
//   * Split up PredicateNode with disjunctive chain (e.g., `PredicateNode(a OR b OR c)`) as their scan expression into
//   * n-1 consecutive UnionNodes and n PredicateNodes.
//   */
//  auto previous_union_node = UnionNode::make(UnionMode::Positions);
//  const auto left_input = predicate_node->left_input();
//  lqp_replace_node(predicate_node, previous_union_node);
//
//  auto new_predicate_node = PredicateNode::make(flat_disjunction[0], left_input);
//  previous_union_node->set_left_input(new_predicate_node);
//  _mergeConjunction(new_predicate_node);
//
//  new_predicate_node = PredicateNode::make(flat_disjunction[1], left_input);
//  previous_union_node->set_right_input(new_predicate_node);
//  _mergeConjunction(new_predicate_node);
//
//  for (auto disjunction_idx = size_t{2}; disjunction_idx < flat_disjunction.size(); ++disjunction_idx) {
//    const auto& predicate_expression = flat_disjunction[disjunction_idx];
//    auto next_union_node = UnionNode::make(UnionMode::Positions);
//    lqp_insert_node(previous_union_node, LQPInputSide::Right, next_union_node);
//
//    new_predicate_node = PredicateNode::make(predicate_expression, left_input);
//    next_union_node->set_right_input(new_predicate_node);
//    _mergeConjunction(new_predicate_node);
//
//    previous_union_node = next_union_node;
//  }
//}

/**
 * Function creates a boolean expression from an lqp. It traverses the passed lqp from top to bottom. However, an lqp is
 * evaluated from bottom to top. This requires that the order in which the translated expressions are added to the
 * output expression is the reverse order of how the nodes are traversed. The subsequent_expression parameter passes the
 * translated expressions to the translation of its children nodes which enables to add the translated expression of
 * child node before its parent node to the output expression.
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression_impl(
  const std::shared_ptr<AbstractLQPNode> &begin, const std::optional<const std::shared_ptr<AbstractLQPNode>> &end,
  const std::optional<const std::shared_ptr<AbstractExpression>> &subsequent_expression) {
    if (end && begin == *end) return nullptr;

    switch (begin->type) {
      case LQPNodeType::Predicate: {
        const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(begin);
        const auto predicate = predicate_node->predicate();
        const auto expression = subsequent_expression ? expression_functional::and_(predicate, *subsequent_expression) : predicate;
        const auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), end, expression);
        if (left_input_expression) {
          std::cout << *left_input_expression << std::endl;
          lqp_remove_node(begin->left_input());
          predicate_node->node_expressions[0] = left_input_expression;
          return left_input_expression;
        } else {
          return expression;
        }
      }

      case LQPNodeType::Union: {
        const auto union_node = std::dynamic_pointer_cast<UnionNode>(begin);
        const auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), end,
                                                                                  std::nullopt);
        const auto right_input_expression =
        lqp_subplan_to_boolean_expression_impl(begin->right_input(), end, std::nullopt);
        if (left_input_expression && right_input_expression) {
          std::cout << *left_input_expression << std::endl;
          std::cout << *right_input_expression << std::endl;
          const auto or_expression = expression_functional::or_(left_input_expression, right_input_expression);
          const auto expression = subsequent_expression ? expression_functional::and_(or_expression, *subsequent_expression) : or_expression;

          std::cout << "prepare merge" << std::endl;
          lqp_remove_node(begin->left_input());
          lqp_remove_node(begin->right_input());
          const auto predicate_node = PredicateNode::make(expression);
          lqp_replace_node(begin, predicate_node);
          Assert(!predicate_node->right_input() || predicate_node->left_input() == predicate_node->right_input(), "The new predicate node must not have two different inputs");
          predicate_node->set_right_input(nullptr);
          std::cout << "merge" << std::endl;
          return expression;
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
  const std::shared_ptr<AbstractLQPNode>& begin, const std::optional<const std::shared_ptr<AbstractLQPNode>>& end) const {
    return lqp_subplan_to_boolean_expression_impl(begin, end, std::nullopt);
  }

void PredicateMergeRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (!node) {
    return;
  }

//  auto top_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  std::set<std::shared_ptr<AbstractLQPNode>> break_nodes;

    visit_lqp(node, [&](const auto& sub_node) {
      switch (sub_node->type) {
        case LQPNodeType::Predicate:
        case LQPNodeType::Union:
//          top_nodes.emplace_back(sub_node);
          return LQPVisitation::VisitInputs;

        default:
          break_nodes.insert(sub_node);
          return LQPVisitation::DoNotVisitInputs;
      }
    });

  std::cout << "BBB: " << break_nodes.size() << std::endl;
  for (const auto& next_node : break_nodes) {
    apply_to(next_node->left_input());
    apply_to(next_node->right_input());
  }

//  // _splitConjunction() and _splitDisjunction() split up logical expressions by calling each other recursively
//  std::cout << "BBB: " << top_nodes.size() << std::endl;
//  for (const auto& top_node : top_nodes) {
  const auto merged_expr = _lqp_subplan_to_boolean_expression(node, std::nullopt);
  if (merged_expr) {
    std::cout << "AAAAA: " << *merged_expr << std::endl;
  }

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
