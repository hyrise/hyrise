#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "lqp_column_reference.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace opossum {

class TableStatistics;

QualifiedColumnName::QualifiedColumnName(const std::string& column_name, const std::optional<std::string>& table_name)
    : column_name(column_name), table_name(table_name) {}

std::string QualifiedColumnName::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type) : _type(node_type) {}

LQPColumnReference AbstractLQPNode::adapt_column_reference_to_different_lqp(
    const LQPColumnReference& column_reference, const std::shared_ptr<AbstractLQPNode>& original_lqp,
    const std::shared_ptr<AbstractLQPNode>& copied_lqp) {
  /**
   * Map a ColumnReference to the same ColumnReference in a different LQP, by
   * (1) Figuring out the ColumnID it has in the original node
   * (2) Returning the ColumnReference at that ColumnID in the copied node
   */
  const auto output_column_id = original_lqp->get_output_column_id(column_reference);
  return copied_lqp->output_column_references()[output_column_id];
}

std::vector<std::shared_ptr<AbstractLQPNode>> AbstractLQPNode::outputs() const {
  std::vector<std::shared_ptr<AbstractLQPNode>> outputs;
  outputs.reserve(_outputs.size());

  for (const auto& output_weak_ptr : _outputs) {
    const auto output = output_weak_ptr.lock();
    DebugAssert(output, "Failed to lock output");
    outputs.emplace_back(output);
  }

  return outputs;
}

std::vector<LQPOutputRelation> AbstractLQPNode::output_relations() const {
  std::vector<LQPOutputRelation> output_relations(output_count());

  const auto outputs = this->outputs();
  const auto input_sides = get_input_sides();

  for (size_t output_idx = 0; output_idx < output_relations.size(); ++output_idx) {
    output_relations[output_idx] = LQPOutputRelation{outputs[output_idx], input_sides[output_idx]};
  }

  return output_relations;
}

size_t AbstractLQPNode::output_count() const { return _outputs.size(); }

void AbstractLQPNode::remove_output(const std::shared_ptr<AbstractLQPNode>& output) {
  const auto input_side = get_input_side(output);
  output->set_input(input_side, nullptr);
}

void AbstractLQPNode::clear_outputs() {
  // Don't use for-each loop here, as remove_output manipulates the _outputs vector
  while (!_outputs.empty()) {
    auto output = _outputs.front().lock();
    DebugAssert(output, "Failed to lock output");
    remove_output(output);
  }
}

LQPInputSide AbstractLQPNode::get_input_side(const std::shared_ptr<AbstractLQPNode>& output) const {
  if (output->_inputs[0].get() == this) {
    return LQPInputSide::Left;
  } else if (output->_inputs[1].get() == this) {
    return LQPInputSide::Right;
  } else {
    Fail("Specified output node is not actually a output node of this node.");
  }
}

std::vector<LQPInputSide> AbstractLQPNode::get_input_sides() const {
  std::vector<LQPInputSide> input_sides;
  input_sides.reserve(_outputs.size());

  for (const auto& output_weak_ptr : _outputs) {
    const auto output = output_weak_ptr.lock();
    DebugAssert(output, "Failed to lock output");
    input_sides.emplace_back(get_input_side(output));
  }

  return input_sides;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::left_input() const { return _inputs[0]; }

void AbstractLQPNode::set_left_input(const std::shared_ptr<AbstractLQPNode>& left) {
  set_input(LQPInputSide::Left, left);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::right_input() const { return _inputs[1]; }

void AbstractLQPNode::set_right_input(const std::shared_ptr<AbstractLQPNode>& right) {
  set_input(LQPInputSide::Right, right);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::input(LQPInputSide side) const {
  const auto input_index = static_cast<int>(side);
  return _inputs[input_index];
}

void AbstractLQPNode::set_input(LQPInputSide side, const std::shared_ptr<AbstractLQPNode>& input) {
  // We need a reference to _inputs[input_idx], so not calling this->input(side)
  auto& current_input = _inputs[static_cast<int>(side)];

  if (current_input == input) {
    return;
  }

  // Untie from previous input
  if (current_input) {
    current_input->_remove_output_pointer(shared_from_this());
  }

  /**
   * Tie in the new input
   */
  current_input = input;
  if (current_input) {
    current_input->_add_output_pointer(shared_from_this());
  }

  _input_changed();
}

LQPNodeType AbstractLQPNode::type() const { return _type; }

bool AbstractLQPNode::subplan_is_read_only() const {
  auto read_only = true;
  if (left_input()) read_only &= left_input()->subplan_is_read_only();
  if (right_input()) read_only &= right_input()->subplan_is_read_only();
  return read_only;
}

bool AbstractLQPNode::subplan_is_validated() const {
  if (type() == LQPNodeType::Validate) return true;

  if (!left_input() && !right_input()) return false;

  auto inputs_validated = true;
  if (left_input()) inputs_validated &= left_input()->subplan_is_validated();
  if (right_input()) inputs_validated &= right_input()->subplan_is_validated();
  return inputs_validated;
}

void AbstractLQPNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _statistics = statistics; }

const std::shared_ptr<TableStatistics> AbstractLQPNode::get_statistics() {
  if (!_statistics) {
    _statistics = derive_statistics_from(left_input(), right_input());
  }

  return _statistics;
}

std::shared_ptr<TableStatistics> AbstractLQPNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input,
              "Default implementation of derive_statistics_from() requires a left input, override in concrete node "
              "implementation for different behavior");
  DebugAssert(!right_input, "Default implementation of derive_statistics_from() cannot have a right_input");

  return left_input->get_statistics();
}

const std::vector<std::string>& AbstractLQPNode::output_column_names() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(left_input() && !right_input(),
              "Node has no or two inputs and therefore needs to override this function.");
  return left_input()->output_column_names();
}

const std::vector<LQPColumnReference>& AbstractLQPNode::output_column_references() const {
  /**
   * Default implementation of output_column_references() will return the ColumnReferences of the left_input if it exists,
   * otherwise will pretend that all Columns originate in this node.
   * Nodes with both inputs need to override this as the default implementation can't cover their behaviour.
   */

  if (!_output_column_references) {
    Assert(!right_input(), "Nodes that have two inputs must override this method");
    if (left_input()) {
      _output_column_references = left_input()->output_column_references();
    } else {
      _output_column_references.emplace();
      for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
        _output_column_references->emplace_back(shared_from_this(), column_id);
      }
    }
  }

  return *_output_column_references;
}

std::optional<LQPColumnReference> AbstractLQPNode::find_column(const QualifiedColumnName& qualified_column_name) const {
  /**
   * If this node carries an alias that is different from that of the NamedColumnReference, we can't resolve the column
   * in this node. E.g. in `SELECT t1.a FROM t1 AS something_else;` "t1.a" can't be resolved since it carries an alias.
   */
  const auto qualified_column_name_without_local_table_name = _resolve_local_table_name(qualified_column_name);
  if (!qualified_column_name_without_local_table_name) {
    return std::nullopt;
  }

  /**
   * If the table name got resolved (i.e., the alias or name of this node equals the table name), look for the Column
   * in the output of this node
   */
  if (!qualified_column_name_without_local_table_name->table_name) {
    for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
      if (output_column_names()[column_id] == qualified_column_name_without_local_table_name->column_name) {
        return output_column_references()[column_id];
      }
    }
    return std::nullopt;
  }

  /**
   * Look for the Column in input nodes
   */
  const auto resolve_qualified_column_name = [&](
      const auto& node, const auto& qualified_column_name) -> std::optional<LQPColumnReference> {
    if (node) {
      const auto column_reference = node->find_column(qualified_column_name);
      if (column_reference) {
        if (find_output_column_id(*column_reference)) {
          return column_reference;
        }
      }
    }
    return std::nullopt;
  };

  const auto column_reference_from_left =
      resolve_qualified_column_name(left_input(), *qualified_column_name_without_local_table_name);
  const auto column_reference_from_right =
      resolve_qualified_column_name(right_input(), *qualified_column_name_without_local_table_name);

  Assert(!column_reference_from_left || !column_reference_from_right ||
             column_reference_from_left == column_reference_from_right,
         "Column '" + qualified_column_name_without_local_table_name->as_string() + "' is ambiguous");

  if (column_reference_from_left) {
    return column_reference_from_left;
  }
  return column_reference_from_right;
}

LQPColumnReference AbstractLQPNode::get_column(const QualifiedColumnName& qualified_column_name) const {
  const auto colum_origin = find_column(qualified_column_name);
  DebugAssert(colum_origin, "Couldn't resolve column origin of " + qualified_column_name.as_string());
  return *colum_origin;
}

std::shared_ptr<const AbstractLQPNode> AbstractLQPNode::find_table_name_origin(const std::string& table_name) const {
  // If this node has an ALIAS that matches the table_name, this is the node we're looking for
  if (_table_alias && *_table_alias == table_name) {
    return shared_from_this();
  }

  // If this node has an alias, it hides the names of tables in its inputs and search does not continue.
  // Also, it does not need to continue if there are no inputs
  if (!left_input() || _table_alias) {
    return nullptr;
  }

  const auto table_name_origin_in_left_input = left_input()->find_table_name_origin(table_name);

  if (right_input()) {
    const auto table_name_origin_in_right_input = right_input()->find_table_name_origin(table_name);

    if (table_name_origin_in_left_input && table_name_origin_in_right_input) {
      // Both inputs could contain the table in the case of a diamond-shaped LQP as produced by an OR.
      // This is legal as long as they ultimately resolve to the same table origin.
      Assert(table_name_origin_in_left_input == table_name_origin_in_right_input,
             "If a node has two inputs, both have to resolve a table name to the same node");
      return table_name_origin_in_left_input;
    } else if (table_name_origin_in_right_input) {
      return table_name_origin_in_right_input;
    }
  }

  return table_name_origin_in_left_input;
}

std::optional<ColumnID> AbstractLQPNode::find_output_column_id(const LQPColumnReference& column_reference) const {
  const auto& output_column_references = this->output_column_references();
  const auto iter = std::find(output_column_references.begin(), output_column_references.end(), column_reference);

  if (iter == output_column_references.end()) {
    return std::nullopt;
  }

  return static_cast<ColumnID>(std::distance(output_column_references.begin(), iter));
}

ColumnID AbstractLQPNode::get_output_column_id(const LQPColumnReference& column_reference) const {
  const auto column_id = find_output_column_id(column_reference);
  Assert(column_id, "Couldn't resolve LQPColumnReference");
  return *column_id;
}

size_t AbstractLQPNode::output_column_count() const { return output_column_names().size(); }

void AbstractLQPNode::remove_from_tree() {
  Assert(!right_input(), "Can only remove nodes that only have a left input or no inputs");

  /**
   * Back up outputs and in which input side they hold this node
   */
  auto outputs = this->outputs();
  auto input_sides = this->get_input_sides();

  /**
   * Hold left_input ptr in extra variable to keep the ref count up and untie it from this node.
   * left_input might be nullptr
   */
  auto left_input = this->left_input();
  set_left_input(nullptr);

  /**
   * Tie this nodes previous outputs with this nodes previous left input
   * If left_input is nullptr, still call set_input so this node will get untied from the LQP.
   */
  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], left_input);
  }
}

void AbstractLQPNode::replace_with(const std::shared_ptr<AbstractLQPNode>& replacement_node) {
  DebugAssert(replacement_node->_outputs.empty(), "Node can't have outputs");
  DebugAssert(!replacement_node->left_input() && !replacement_node->right_input(), "Node can't have inputs");

  const auto outputs = this->outputs();
  const auto input_sides = this->get_input_sides();

  /**
   * Tie the replacement_node with this nodes inputs
   */
  replacement_node->set_left_input(left_input());
  replacement_node->set_right_input(right_input());

  /**
   * Tie the replacement_node with this nodes outputs. This will effectively perform clear_outputs() on this node.
   */
  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], replacement_node);
  }

  /**
   * Untie this node from the LQP
   */
  set_left_input(nullptr);
  set_right_input(nullptr);
}

void AbstractLQPNode::set_alias(const std::optional<std::string>& table_alias) { _table_alias = table_alias; }

void AbstractLQPNode::print(std::ostream& out) const {
  const auto get_inputs_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractLQPNode>> inputs;
    if (node->left_input()) inputs.emplace_back(node->left_input());
    if (node->right_input()) inputs.emplace_back(node->right_input());
    return inputs;
  };
  const auto node_print_fn = [](const auto& node, auto& stream) {
    stream << node->description();

    if (node->_table_alias) {
      stream << " -- ALIAS: '" << *node->_table_alias << "'";
    }
  };

  print_directed_acyclic_graph<const AbstractLQPNode>(shared_from_this(), get_inputs_fn, node_print_fn, out);
}

std::string AbstractLQPNode::get_verbose_column_name(ColumnID column_id) const {
  DebugAssert(!right_input(), "Node with right input needs to override this function.");

  /**
   *  A AbstractLQPNode without a left input should generally be a StoredTableNode, which overrides this function. But
   *  since get_verbose_column_name() is just a convenience function we don't want to force anyone to override this
   *  function when experimenting with nodes. Thus we handle the case of no left input here as well.
   */
  if (!left_input()) {
    if (_table_alias) {
      return *_table_alias + "." + output_column_names()[column_id];
    }

    return output_column_names()[column_id];
  }

  const auto verbose_name = left_input()->get_verbose_column_name(column_id);

  if (_table_alias) {
    return *_table_alias + "." + verbose_name;
  }

  return verbose_name;
}

std::vector<std::string> AbstractLQPNode::get_verbose_column_names() const {
  std::vector<std::string> verbose_names(output_column_count());
  for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
    verbose_names[column_id] = get_verbose_column_name(column_id);
  }
  return verbose_names;
}

std::optional<QualifiedColumnName> AbstractLQPNode::_resolve_local_table_name(
    const QualifiedColumnName& qualified_column_name) const {
  if (qualified_column_name.table_name && _table_alias) {
    if (*qualified_column_name.table_name == *_table_alias) {
      // The used table name is the alias of this table. Remove id from the QualifiedColumnName for further search
      auto reference_without_local_alias = qualified_column_name;
      reference_without_local_alias.table_name = std::nullopt;
      return reference_without_local_alias;
    } else {
      return {};
    }
  }
  return qualified_column_name;
}

void AbstractLQPNode::_input_changed() {
  _statistics.reset();
  _output_column_references.reset();

  _on_input_changed();
  for (auto& output : outputs()) {
    output->_input_changed();
  }
}

void AbstractLQPNode::_remove_output_pointer(const std::shared_ptr<AbstractLQPNode>& output) {
  const auto iter =
      std::find_if(_outputs.begin(), _outputs.end(), [&](const auto& other) { return output == other.lock(); });
  DebugAssert(iter != _outputs.end(), "Specified output node is not actually a output node of this node.");

  /**
   * TODO(anybody) This is actually a O(n) operation, could be O(1) by just swapping the last element into the deleted
   * element.
   */
  _outputs.erase(iter);
}

void AbstractLQPNode::_add_output_pointer(const std::shared_ptr<AbstractLQPNode>& output) {
  // Having the same output multiple times is allowed, e.g. for self joins
  _outputs.emplace_back(output);
}

std::shared_ptr<LQPExpression> AbstractLQPNode::adapt_expression_to_different_lqp(
    const std::shared_ptr<LQPExpression>& expression, const std::shared_ptr<AbstractLQPNode>& original_lqp,
    const std::shared_ptr<AbstractLQPNode>& copied_lqp) {
  if (!expression) return nullptr;

  if (expression->type() == ExpressionType::Column) {
    expression->set_column_reference(
        adapt_column_reference_to_different_lqp(expression->column_reference(), original_lqp, copied_lqp));
  }

  for (auto& argument_expression : expression->aggregate_function_arguments()) {
    adapt_expression_to_different_lqp(argument_expression, original_lqp, copied_lqp);
  }

  adapt_expression_to_different_lqp(expression->left_child(), original_lqp, copied_lqp);
  adapt_expression_to_different_lqp(expression->right_child(), original_lqp, copied_lqp);

  return expression;
}

std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
AbstractLQPNode::find_first_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& rhs) const {
  return _find_first_subplan_mismatch_impl(shared_from_this(), rhs);
}

std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
AbstractLQPNode::_find_first_subplan_mismatch_impl(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                   const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (lhs == rhs) return std::nullopt;
  if (static_cast<bool>(lhs) != static_cast<bool>(rhs)) return std::make_pair(lhs, rhs);
  if (lhs->type() != rhs->type()) return std::make_pair(lhs, rhs);

  if (!lhs->shallow_equals(*rhs)) return std::make_pair(lhs, rhs);

  const auto left_input_mismatch = _find_first_subplan_mismatch_impl(lhs->left_input(), rhs->left_input());
  if (left_input_mismatch) return left_input_mismatch;

  const auto right_input_mismatch = _find_first_subplan_mismatch_impl(lhs->right_input(), rhs->right_input());
  if (right_input_mismatch) return right_input_mismatch;

  return std::nullopt;
}

bool AbstractLQPNode::_equals(const AbstractLQPNode& lqp_left,
                              const std::vector<std::shared_ptr<LQPExpression>>& expressions_left,
                              const AbstractLQPNode& lqp_right,
                              const std::vector<std::shared_ptr<LQPExpression>>& expressions_right) {
  if (expressions_left.size() != expressions_right.size()) return false;

  for (size_t expression_idx = 0; expression_idx < expressions_left.size(); ++expression_idx) {
    if (!_equals(lqp_left, expressions_left[expression_idx], lqp_right, expressions_right[expression_idx]))
      return false;
  }

  return true;
}

bool AbstractLQPNode::_equals(const AbstractLQPNode& lqp_left,
                              const std::shared_ptr<const LQPExpression>& expression_left,
                              const AbstractLQPNode& lqp_right,
                              const std::shared_ptr<const LQPExpression>& expression_right) {
  if (!expression_left && !expression_right) return true;
  if (!expression_left || !expression_right) return false;
  if (*expression_left == *expression_right) return true;
  if (expression_left->type() != expression_right->type()) return false;
  if (expression_left->aggregate_function_arguments().size() != expression_right->aggregate_function_arguments().size())
    return false;

  const auto type = expression_left->type();

  if (expression_left->alias() != expression_right->alias()) return false;

  if (type == ExpressionType::Column) {
    return _equals(lqp_left, expression_left->column_reference(), lqp_right, expression_right->column_reference());
  }

  if (type == ExpressionType::Function) {
    if (expression_left->aggregate_function() != expression_right->aggregate_function()) return false;

    for (size_t arg_idx = 0; arg_idx < expression_left->aggregate_function_arguments().size(); ++arg_idx) {
      if (!_equals(lqp_left, expression_left->aggregate_function_arguments()[arg_idx], lqp_right,
                   expression_right->aggregate_function_arguments()[arg_idx]))
        return false;
    }
  }

  if (!_equals(lqp_left, expression_left->left_child(), lqp_right, expression_right->left_child())) return false;
  if (!_equals(lqp_left, expression_left->right_child(), lqp_right, expression_right->right_child())) return false;

  return true;
}

bool AbstractLQPNode::_equals(const AbstractLQPNode& lqp_left,
                              const std::vector<LQPColumnReference>& column_references_left,
                              const AbstractLQPNode& lqp_right,
                              const std::vector<LQPColumnReference>& column_references_right) {
  if (column_references_left.size() != column_references_right.size()) return false;
  for (size_t column_reference_idx = 0; column_reference_idx < column_references_left.size(); ++column_reference_idx) {
    if (!_equals(lqp_left, column_references_left[column_reference_idx], lqp_right,
                 column_references_right[column_reference_idx]))
      return false;
  }
  return true;
}

bool AbstractLQPNode::_equals(const AbstractLQPNode& lqp_left, const LQPColumnReference& column_reference_left,
                              const AbstractLQPNode& lqp_right, const LQPColumnReference& column_reference_right) {
  // We just need a temporary ColumnReference which won't be used to manipulate nodes, promised.
  auto& mutable_lqp_left = const_cast<AbstractLQPNode&>(lqp_left);
  auto& mutable_lqp_right = const_cast<AbstractLQPNode&>(lqp_right);
  const auto column_reference_left_adapted_to_right = AbstractLQPNode::adapt_column_reference_to_different_lqp(
      column_reference_left, mutable_lqp_left.shared_from_this(), mutable_lqp_right.shared_from_this());

  return column_reference_left_adapted_to_right == column_reference_right;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy() const {
  PreviousCopiesMap previous_copies;
  return _deep_copy(previous_copies);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_deep_copy(PreviousCopiesMap& previous_copies) const {
  if (auto it = previous_copies.find(shared_from_this()); it != previous_copies.cend()) {
    return it->second;
  }

  auto copied_left_input = left_input() ? left_input()->_deep_copy(previous_copies) : nullptr;
  auto copied_right_input = right_input() ? right_input()->_deep_copy(previous_copies) : nullptr;

  // We cannot use the copy constructor here, because it does not work with shared_from_this()
  auto deep_copy = _deep_copy_impl(copied_left_input, copied_right_input);
  if (copied_left_input) deep_copy->set_left_input(copied_left_input);
  if (copied_right_input) deep_copy->set_right_input(copied_right_input);

  previous_copies.emplace(shared_from_this(), deep_copy);

  return deep_copy;
}

}  // namespace opossum
