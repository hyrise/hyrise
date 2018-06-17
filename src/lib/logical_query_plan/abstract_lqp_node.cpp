#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"
#include "lqp_utils.hpp"

using namespace std::string_literals;

namespace opossum {

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type) : type(node_type) {}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::left_input() const { return _inputs[0]; }

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::right_input() const { return _inputs[1]; }

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::input(LQPInputSide side) const {
  const auto input_index = static_cast<int>(side);
  return _inputs[input_index];
}

void AbstractLQPNode::set_left_input(const std::shared_ptr<AbstractLQPNode>& left) {
  set_input(LQPInputSide::Left, left);
}

void AbstractLQPNode::set_right_input(const std::shared_ptr<AbstractLQPNode>& right) {
  set_input(LQPInputSide::Right, right);
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
}

size_t AbstractLQPNode::input_count() const {
  /**
   * Testing the shared_ptrs for null in _inputs to determine input count
   */
  return _inputs.size() - std::count(_inputs.cbegin(), _inputs.cend(), nullptr);
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

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy(LQPNodeMapping node_mapping) const {
  return _deep_copy_impl(node_mapping);
}

bool AbstractLQPNode::shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  if (type != rhs.type) return false;
  return _shallow_equals_impl(rhs, node_mapping);
}

const std::vector<std::shared_ptr<AbstractExpression>>& AbstractLQPNode::output_column_expressions() const {
  Assert(left_input() && !right_input(), "Can only forward input expressions, if there is only a left input");
  return left_input()->output_column_expressions();
}

std::optional<ColumnID> AbstractLQPNode::find_column_id(const AbstractExpression &expression) const {
  const auto& output_column_expressions = this->output_column_expressions(); // Avoid redundant retrieval in loop below
  for (auto column_id = ColumnID{0}; column_id < output_column_expressions.size(); ++column_id) {
    if (output_column_expressions[column_id]->deep_equals(expression)) return column_id;
  }
  return std::nullopt;
}

ColumnID AbstractLQPNode::get_column_id(const AbstractExpression &expression) const {
  const auto column_id = find_column_id(expression);
  Assert(column_id, "Couldn't resolve column '"s + expression.as_column_name() + "'");
  return *column_id;
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

void AbstractLQPNode::print(std::ostream& out) const {
  const auto get_inputs_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractLQPNode>> inputs;
    if (node->left_input()) inputs.emplace_back(node->left_input());
    if (node->right_input()) inputs.emplace_back(node->right_input());
    return inputs;
  };
  const auto node_print_fn = [](const auto& node, auto& stream) {
    stream << node->description();
  };

  print_directed_acyclic_graph<const AbstractLQPNode>(shared_from_this(), get_inputs_fn, node_print_fn, out);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_deep_copy_impl(LQPNodeMapping & node_mapping) const {
  std::shared_ptr<AbstractLQPNode> copied_left_input, copied_right_input;

  if (left_input()) copied_left_input = left_input()->_deep_copy_impl(node_mapping);
  if (right_input()) copied_right_input = right_input()->_deep_copy_impl(node_mapping);

  const auto copy = _shallow_copy(node_mapping);
  copy->set_left_input(copied_left_input);
  copy->set_right_input(copied_right_input);

  return copy;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_shallow_copy(LQPNodeMapping & node_mapping) const {
  const auto node_mapping_iter = node_mapping.find(shared_from_this());

  // Handle diamond shapes in the LQP; don't copy nodes twice
  if (node_mapping_iter != node_mapping.end()) return node_mapping_iter->second;

  auto shallow_copy = _shallow_copy_impl(node_mapping);
  node_mapping.emplace(shared_from_this(), shallow_copy);

  return shallow_copy;
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

}  // namespace opossum
