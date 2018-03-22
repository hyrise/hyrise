#include "predicate_pushdown_rule.hpp"
#include "all_parameter_variant.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"

namespace opossum {

std::string PredicatePushdownRule::name() const { return "Predicate Pushdown Rule"; }

bool PredicatePushdownRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto outputs = node->outputs();
  // Only predicates with exactly one output are currently supported.
  if (outputs.empty() || outputs.size() > 1) {
    return false;
  }

  // find predicate node
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  auto input = node->left_input();
  while (input->type() == LQPNodeType::Predicate) {
    input = input->left_input();
  }

  if (input->type() == LQPNodeType::Join) {
    const auto join_node = std::dynamic_pointer_cast<JoinNode>(input);

    const bool demotable =
        join_node->join_mode() == JoinMode::Inner && _predicate_value_demotable(predicate_node, join_node);

    if (!demotable) {
      return _apply_to_inputs(node);
    }

    node->remove_from_tree();

    const auto predicate_column = predicate_node->column_reference();

    if (_contained_in_left_subtree(join_node, predicate_column)) {
      const auto previous_left_input = join_node->left_input();
      join_node->set_left_input(node);
      node->set_left_input(previous_left_input);
    } else if (_contained_in_right_subtree(join_node, predicate_column)) {
      const auto previous_right_input = join_node->right_input();
      join_node->set_right_input(node);
      node->set_left_input(previous_right_input);
    } else {
      DebugAssert(false, "Predicate denotion failed. The join node must be contained in one subtree.")
    }

    return true;

    // push always down if other node is a sort node
  } else if (input->type() == LQPNodeType::Sort) {
    const auto sort_node = std::dynamic_pointer_cast<SortNode>(input);

    node->remove_from_tree();
    const auto previous_left_input = sort_node->left_input();

    sort_node->set_left_input(node);
    node->set_left_input(previous_left_input);

    return true;
  }
  return false;
}

bool PredicatePushdownRule::_predicate_value_demotable(const std::shared_ptr<PredicateNode>& predicate_node,
                                                   const std::shared_ptr<AbstractLQPNode>& node) const {
  // The predicate must not be demoted if it combines columns of both join partners.
  // This can happen if the value to compare against is indeed another column ID
  // which references a table different from the filtered column's table.
  // This would also apply to value2 (representing BETWEEN queries) if it was not restricted to literal values.

  if (is_lqp_column_reference(predicate_node->value())) {
    const LQPColumnReference value = boost::get<LQPColumnReference>(predicate_node->value());

    const bool all_left =
        _contained_in_left_subtree(node, predicate_node->column_reference()) && _contained_in_left_subtree(node, value);

    const bool all_right =
        _contained_in_right_subtree(node, predicate_node->column_reference()) &&
        _contained_in_right_subtree(node, value);

    return all_left ^ all_right;
  }

  return true;
}

bool PredicatePushdownRule::_contained_in_left_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                                       const LQPColumnReference& column) const {
  Assert(node->left_input(), "Node must have left input to compare column to");
  return _contained_in_node(node->left_input(), column);
}

bool PredicatePushdownRule::_contained_in_right_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                                        const LQPColumnReference& column) const {
  if (node->right_input()) {
    return _contained_in_node(node->right_input(), column);
  }
  return false;
}

bool PredicatePushdownRule::_contained_in_node(const std::shared_ptr<AbstractLQPNode>& node,
                                               const LQPColumnReference& column) const {
  const auto& columns = node->output_column_references();
  return std::find(columns.cbegin(), columns.cend(), column) != columns.cend();
}
}  // namespace opossum
