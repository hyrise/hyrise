#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <unordered_map>

#include "boost/functional/hash.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "join_node.hpp"
#include "lqp_utils.hpp"
#include "predicate_node.hpp"
#include "update_node.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

void collect_lqps_in_plan(const AbstractLQPNode& lqp, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& lqps);

/**
 * Utility for operator<<(std::ostream, AbstractLQPNode)
 * Put all LQPs found in an @param expression into @param lqps
 */
void collect_lqps_from_expression(const std::shared_ptr<AbstractExpression>& expression,
                                  std::unordered_set<std::shared_ptr<AbstractLQPNode>>& lqps) {
  visit_expression(expression, [&](const auto& sub_expression) {
    const auto subquery_expression = std::dynamic_pointer_cast<const LQPSubqueryExpression>(sub_expression);
    if (!subquery_expression) return ExpressionVisitation::VisitArguments;
    lqps.emplace(subquery_expression->lqp);
    collect_lqps_in_plan(*subquery_expression->lqp, lqps);
    return ExpressionVisitation::VisitArguments;
  });
}

/**
 * Utility for operator<<(std::ostream, AbstractLQPNode)
 * Put all LQPs found in expressions in plan @param lqp into @param lqps
 */
void collect_lqps_in_plan(const AbstractLQPNode& lqp, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& lqps) {
  for (const auto& node_expression : lqp.node_expressions) {
    collect_lqps_from_expression(node_expression, lqps);
  }

  if (lqp.left_input()) collect_lqps_in_plan(*lqp.left_input(), lqps);
  if (lqp.right_input()) collect_lqps_in_plan(*lqp.right_input(), lqps);
}

}  // namespace

namespace opossum {

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type,
                                 const std::vector<std::shared_ptr<AbstractExpression>>& node_expressions)
    : type(node_type), node_expressions(node_expressions) {}

AbstractLQPNode::~AbstractLQPNode() {
  Assert(
      _outputs.empty(),
      "Bug detected. There are outputs that should still reference to this node. Thus this node shouldn't get deleted");

  // We're in the destructor, thus we must make sure we're not calling any virtual methods - so we're doing the removal
  // directly instead of calling set_input_left/right(nullptr)
  if (_inputs[0]) _inputs[0]->_remove_output_pointer(*this);
  if (_inputs[1]) _inputs[1]->_remove_output_pointer(*this);
}

size_t AbstractLQPNode::hash() const {
  size_t hash{0};

  visit_lqp(shared_from_this(), [&hash](const auto& node) {
    if (node) {
      for (const auto& expression : node->node_expressions) {
        boost::hash_combine(hash, expression->hash());
      }
      boost::hash_combine(hash, node->type);
      boost::hash_combine(hash, node->_shallow_hash());
      return LQPVisitation::VisitInputs;
    } else {
      return LQPVisitation::DoNotVisitInputs;
    }
  });

  return hash;
}

size_t AbstractLQPNode::_shallow_hash() const { return 0; }

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
  DebugAssert(
      right == nullptr || type == LQPNodeType::Join || type == LQPNodeType::Union || type == LQPNodeType::Update,
      "This node type does not accept a right input");
  set_input(LQPInputSide::Right, right);
}

void AbstractLQPNode::set_input(LQPInputSide side, const std::shared_ptr<AbstractLQPNode>& input) {
  DebugAssert(side == LQPInputSide::Left || input == nullptr || type == LQPNodeType::Join ||
                  type == LQPNodeType::Union || type == LQPNodeType::Update,
              "This node type does not accept a right input");

  // We need a reference to _inputs[input_idx], so not calling this->input(side)
  auto& current_input = _inputs[static_cast<int>(side)];

  if (current_input == input) {
    return;
  }

  // Untie from previous input
  if (current_input) {
    current_input->_remove_output_pointer(*this);
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
    Fail("Specified output node is not actually an output node of this node.");
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
  // set_input() will untie the nodes
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

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy(LQPNodeMapping input_node_mapping) const {
  return _deep_copy_impl(input_node_mapping);
}

bool AbstractLQPNode::shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  if (type != rhs.type) return false;
  return _on_shallow_equals(rhs, node_mapping);
}

const std::vector<std::shared_ptr<AbstractExpression>>& AbstractLQPNode::column_expressions() const {
  Assert(left_input() && !right_input(),
         "Can only forward input expressions iff there is a left input and no right input");
  return left_input()->column_expressions();
}

std::optional<ColumnID> AbstractLQPNode::find_column_id(const AbstractExpression& expression) const {
  const auto& column_expressions = this->column_expressions();  // Avoid redundant retrieval in loop below
  for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); ++column_id) {
    if (*column_expressions[column_id] == expression) return column_id;
  }
  return std::nullopt;
}

ColumnID AbstractLQPNode::get_column_id(const AbstractExpression& expression) const {
  const auto column_id = find_column_id(expression);
  Assert(column_id, "This node has no column '"s + expression.as_column_name() + "'");
  return *column_id;
}

bool AbstractLQPNode::is_column_nullable(const ColumnID column_id) const {
  // Default behaviour: Forward from input
  Assert(left_input() && !right_input(),
         "Can forward nullability from input iff there is a left input and no right input");
  return left_input()->is_column_nullable(column_id);
}

bool AbstractLQPNode::operator==(const AbstractLQPNode& rhs) const {
  return !lqp_find_subplan_mismatch(shared_from_this(), rhs.shared_from_this());
}

bool AbstractLQPNode::operator!=(const AbstractLQPNode& rhs) const { return !operator==(rhs); }

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_deep_copy_impl(LQPNodeMapping& node_mapping) const {
  std::shared_ptr<AbstractLQPNode> copied_left_input, copied_right_input;

  if (left_input()) copied_left_input = left_input()->_deep_copy_impl(node_mapping);
  if (right_input()) copied_right_input = right_input()->_deep_copy_impl(node_mapping);

  const auto copy = _shallow_copy(node_mapping);
  copy->set_left_input(copied_left_input);
  copy->set_right_input(copied_right_input);

  return copy;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto node_mapping_iter = node_mapping.find(shared_from_this());

  // Handle diamond shapes in the LQP; don't copy nodes twice
  if (node_mapping_iter != node_mapping.end()) return node_mapping_iter->second;

  auto shallow_copy = _on_shallow_copy(node_mapping);
  node_mapping.emplace(shared_from_this(), shallow_copy);

  return shallow_copy;
}

void AbstractLQPNode::_remove_output_pointer(const AbstractLQPNode& output) {
  const auto iter = std::find_if(_outputs.begin(), _outputs.end(), [&](const auto& other) {
    /**
     * HACK!
     *  Normally we'd just check `&output == other.lock().get()` here.
     *  BUT (this is the hacky part), we're checking for `other.expired()` here as well and accept an expired element as
     *  a match. If nothing else breaks the only way we might get an expired element is if `other` is the
     *  expired weak_ptr<> to `output` - and thus the element we're looking for - in the following scenario:
     *
     * auto node_a = Node::make()
     * auto node_b = Node::make(..., node_a)
     *
     * node_b.reset(); // node_b::~AbstractLQPNode() will call `node_a.remove_output_pointer(node_b)`
     *                 // But we can't lock node_b anymore, since its ref count is already 0
     */
    return &output == other.lock().get() || other.expired();
  });
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

std::ostream& operator<<(std::ostream& stream, const AbstractLQPNode& node) {
  // Recursively collect all LQPs in LQPSubqueryExpressions (and any anywhere within those) in this LQP into a list and
  // then print them
  auto lqps = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_lqps_in_plan(node, lqps);

  const auto output_lqp_to_stream = [&](const auto& root) {
    const auto get_inputs_fn = [](const auto& node2) {
      std::vector<std::shared_ptr<const AbstractLQPNode>> inputs;
      if (node2->left_input()) inputs.emplace_back(node2->left_input());
      if (node2->right_input()) inputs.emplace_back(node2->right_input());
      return inputs;
    };

    const auto node_print_fn = [](const auto& node2, auto& stream2) {
      stream2 << node2->description();
      if (!node2->comment.empty()) {
        stream2 << " (" << node2->comment << ")";
      }
    };

    print_directed_acyclic_graph<const AbstractLQPNode>(root.shared_from_this(), get_inputs_fn, node_print_fn, stream);
  };

  output_lqp_to_stream(node);

  if (lqps.empty()) return stream;

  stream << "-------- Subqueries ---------" << std::endl;

  for (const auto& lqp : lqps) {
    stream << lqp.get() << ": " << std::endl;
    output_lqp_to_stream(*lqp);
    stream << std::endl;
  }

  return stream;
}

}  // namespace opossum
