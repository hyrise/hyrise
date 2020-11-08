#include "predicate_split_up_rule.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

namespace {
bool predicates_are_mutually_exclusive(const std::vector<std::shared_ptr<AbstractExpression>>& predicates) {
  // Optimization: The ExpressionReductionRule transforms `x NOT LIKE 'foo%'` into `x < 'foo' OR x >= 'fop'`. For this
  // special case, we know that the two OR arguments are mutually exclusive. In this case, we do not need to use
  // SetOperationMode::Positions but can use SetOperationMode::All. For now, we only cover cases with less-than and
  // greater-than-equals. There are more cases, too, but those are not covered yet. For example, the optimization
  // expects the two predicates in the order mentioned above. Also, it does not cover less-than/greater-than (not
  // greater-than-equals)
  //
  // Note that this is not equal to XOR. In the case handled here, we know that both sides cannot be true at the same
  // time. XOR would require us to check if both are true and discard the row if they are. As such, even if we later
  // introduce PredicateCondition::Xor, we cannot automatically use ::All for that expression.
  if (predicates.size() != 2) return false;

  const auto first_binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicates[0]);
  const auto second_binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicates[1]);
  if (!first_binary_predicate || !second_binary_predicate) return false;

  // Check for pattern `col_x < 'val' OR col_x >= 'vam'`
  if (first_binary_predicate->predicate_condition != PredicateCondition::LessThan ||
      second_binary_predicate->predicate_condition != PredicateCondition::GreaterThanEquals) {
    // Wrong predicates
    return false;
  }

  if (first_binary_predicate->left_operand()->type != ExpressionType::LQPColumn ||
      first_binary_predicate->left_operand() != second_binary_predicate->left_operand()) {
    // Left side of predicates is not a column or is a different column for both expressions
    return false;
  }

  if (first_binary_predicate->right_operand()->type != ExpressionType::Value ||
      second_binary_predicate->right_operand()->type != ExpressionType::Value) {
    // Right side of predicates is not a value
    return false;
  }

  if (first_binary_predicate->right_operand()->data_type() != second_binary_predicate->right_operand()->data_type()) {
    // Different data types - to keep things simple, we do not handle this case here.
    return false;
  }

  const auto& first_value_expression = static_cast<const ValueExpression&>(*first_binary_predicate->right_operand());
  const auto& second_value_expression = static_cast<const ValueExpression&>(*second_binary_predicate->right_operand());

  auto first_less_than_second = false;
  boost::apply_visitor(
      [&](const auto first_value) {
        const auto second_value = boost::get<decltype(first_value)>(second_value_expression.value);
        first_less_than_second = first_value < second_value;
      },
      first_value_expression.value);

  return first_less_than_second;
}
}  // namespace

PredicateSplitUpRule::PredicateSplitUpRule(const bool split_disjunctions) : _split_disjunctions(split_disjunctions) {}

void PredicateSplitUpRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateSplitUpRule needs root to hold onto");

  auto predicate_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
  visit_lqp(root, [&](const auto& sub_node) {
    if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(sub_node)) {
      predicate_nodes.emplace_back(predicate_node);
    }
    return LQPVisitation::VisitInputs;
  });

  // _split_conjunction() and _split_disjunction() split up logical expressions by calling each other recursively.
  for (const auto& predicate_node : predicate_nodes) {
    _split_conjunction(predicate_node);
    if (_split_disjunctions) _split_disjunction(predicate_node);
  }
}

/**
 * Split up a single PredicateNode with a conjunctive chain as its scan expression into multiple consecutive
 * PredicateNodes.
 *
 * EXAMPLE:
 *                |                                 |
 *   PredicateNode(a AND b AND c)             PredicateNode(c)
 *                |                                 |
 *                |               ----->      PredicateNode(b)
 *                |                                 |
 *                |                           PredicateNode(a)
 *                |                                 |
 *              Table                             Table
 */
void PredicateSplitUpRule::_split_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  const auto flat_conjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);
  if (flat_conjunction.size() <= 1) {
    return;
  }

  for (const auto& predicate_expression : flat_conjunction) {
    const auto& new_predicate_node = PredicateNode::make(predicate_expression);
    lqp_insert_node(predicate_node, LQPInputSide::Left, new_predicate_node);
    if (_split_disjunctions) {
      _split_disjunction(new_predicate_node);
    }
  }
  lqp_remove_node(predicate_node);
}

/**
 * Split up a single PredicateNode with a disjunctive chain (e.g., `PredicateNode(a OR b OR c)`) as its scan expression
 * into n-1 consecutive UnionNodes and n PredicateNodes.
 *
 * EXAMPLE:
 *
 *           |                                       |                                            |
 * PredicateNode(a OR b OR c)                    __Union___                                   __Union___
 *           |                                  /          \                                 /          \
 *           |                            Predicate(a)     |                           Predicate(a)     |
 *           |                                 |           |                                |           |
 *           |                                 |     Predicate(b OR c)                      |       __Union__
 *           |                ----->           |           |                ----->          |      /         \
 *           |                                 |           |                                | Predicate(b)   |
 *           |                                 |           |                                |     |          |
 *           |                                 |           |                                |     |     Predicate(c)
 *           |                                 |           |                                \     \         /
 *         Table                               \--Table---/                                 \----Table-----/
 */
void PredicateSplitUpRule::_split_disjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  const auto flat_disjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::Or);
  if (flat_disjunction.size() <= 1) {
    return;
  }

  // Step 1: Insert initial diamond
  const auto set_operation_mode =
      predicates_are_mutually_exclusive(flat_disjunction) ? SetOperationMode::All : SetOperationMode::Positions;
  auto top_union_node = UnionNode::make(set_operation_mode);
  const auto diamond_bottom = predicate_node->left_input();
  lqp_replace_node(predicate_node, top_union_node);
  {
    const auto new_predicate_node = PredicateNode::make(flat_disjunction[0], diamond_bottom);
    top_union_node->set_left_input(new_predicate_node);
    _split_conjunction(new_predicate_node);
  }
  {
    const auto new_predicate_node = PredicateNode::make(flat_disjunction[1], diamond_bottom);
    top_union_node->set_right_input(new_predicate_node);
    _split_conjunction(new_predicate_node);
  }

  // Step 2: Insert all remaining n-2 UnionNodes into the diamond subplan. Each UnionNode becomes the right input of the
  // higher UnionNode, respectively. The left inputs become PredicateNodes that all have the same input
  // (diamond_bottom).
  auto previous_union_node = top_union_node;
  for (auto disjunction_idx = size_t{2}; disjunction_idx < flat_disjunction.size(); ++disjunction_idx) {
    const auto& predicate_expression = flat_disjunction[disjunction_idx];
    const auto new_union_node = UnionNode::make(SetOperationMode::Positions);
    lqp_insert_node(previous_union_node, LQPInputSide::Right, new_union_node);

    const auto new_predicate_node = PredicateNode::make(predicate_expression, diamond_bottom);
    new_union_node->set_right_input(new_predicate_node);
    _split_conjunction(new_predicate_node);

    previous_union_node = new_union_node;
  }
}

}  // namespace opossum
