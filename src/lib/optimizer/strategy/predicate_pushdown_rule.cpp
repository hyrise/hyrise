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
  // find predicate node
  if (node->type() == LQPNodeType::Predicate) {
    const auto parents = node->parents();
    if (parents.empty() || parents.size() > 1) {
      return false;
    }

    const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

    // can predicate node demoted?
    auto child = node->left_child();
    while (child->type() == LQPNodeType::Predicate) {
      child = child->left_child();
    }

    // can be demoted?
    if (child->type() == LQPNodeType::Join) {
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(child);

      const bool valid = join_node->join_mode() == JoinMode::Inner && _predicate_value_valid(predicate_node, join_node);

      if (valid) {
        node->remove_from_tree();

        const auto predicate_column = predicate_node->column_reference();
        if (_contained_in_left_subtree(join_node, predicate_column)) {
          const auto prev_left_child = join_node->left_child();
          join_node->set_left_child(node);
          node->set_left_child(prev_left_child);
        } else if (_contained_in_right_subtree(join_node, predicate_column)) {
          const auto prev_right_child = join_node->right_child();
          join_node->set_right_child(node);
          node->set_left_child(prev_right_child);
        }

        return true;
      }
      // push always down if other node is a sort node
    } else if (child->type() == LQPNodeType::Sort) {
      const auto sort_node = std::dynamic_pointer_cast<SortNode>(child);

      node->remove_from_tree();
      const auto prev_left_child = sort_node->left_child();

      sort_node->set_left_child(node);
      node->set_left_child(prev_left_child);

      return true;
    }
  }

  return _apply_to_children(node);
  // how far can the predicate node demoted?
  // actually demote predicate node one or more levels
}

bool PredicatePushdownRule::_predicate_value_valid(const std::shared_ptr<PredicateNode>& predicate_node,
                                                   const std::shared_ptr<AbstractLQPNode>& node) const {
  // The predicate must not be demoted if it combines columns of both join partners.
  // This can happen if the value to compare against is indeed another column ID
  // which references a table different from the filtered column's table.
  // This would also apply to value2 (representing BETWEEN queries) if it was not restricted to literal values.

  if (is_lqp_column_reference(predicate_node->value())) {
    const LQPColumnReference value = boost::get<LQPColumnReference>(predicate_node->value());

    const bool all_left =
        _contained_in_left_subtree(node, predicate_node->column_reference()) && _contained_in_left_subtree(node, value);

    const bool all_right = _contained_in_right_subtree(node, predicate_node->column_reference()) &&
                           _contained_in_right_subtree(node, value);

    return all_left ^ all_right;
  }

  return true;
}

bool PredicatePushdownRule::_contained_in_left_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                                       const LQPColumnReference& column) const {
  Assert(node->left_child(), "must have a left child");
  return _contained_in_node(node->left_child(), column);
}

bool PredicatePushdownRule::_contained_in_right_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                                        const LQPColumnReference& column) const {
  if (node->right_child()) {
    return _contained_in_node(node->right_child(), column);
  }
  return false;
}

bool PredicatePushdownRule::_contained_in_node(const std::shared_ptr<AbstractLQPNode>& node,
                                               const LQPColumnReference& column) const {
  const auto& columns = node->output_column_references();
  return std::find(columns.cbegin(), columns.cend(), column) != columns.cend();
}
}  // namespace opossum
