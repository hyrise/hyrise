#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <boost/container_hash/hash.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "lqp_utils.hpp"
#include "predicate_node.hpp"
#include "update_node.hpp"
#include "utils/assert.hpp"
#include "utils/map_prunable_subquery_predicates.hpp"
#include "utils/print_utils.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void collect_lqps_in_plan(const AbstractLQPNode& lqp, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& lqps);

/**
 * Utility for operator<<(std::ostream, AbstractLQPNode)
 * Put all LQPs found in an @param expression into @param lqps
 */
void collect_lqps_from_expression(const std::shared_ptr<AbstractExpression>& expression,
                                  std::unordered_set<std::shared_ptr<AbstractLQPNode>>& lqps) {
  visit_expression(expression, [&](const auto& sub_expression) {
    const auto subquery_expression = std::dynamic_pointer_cast<const LQPSubqueryExpression>(sub_expression);
    if (!subquery_expression) {
      return ExpressionVisitation::VisitArguments;
    }

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

  if (lqp.left_input()) {
    collect_lqps_in_plan(*lqp.left_input(), lqps);
  }

  if (lqp.right_input()) {
    collect_lqps_in_plan(*lqp.right_input(), lqps);
  }
}

}  // namespace

namespace hyrise {

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type,
                                 const std::vector<std::shared_ptr<AbstractExpression>>& init_node_expressions)
    : type(node_type), node_expressions(init_node_expressions) {}

AbstractLQPNode::~AbstractLQPNode() {
  Assert(_outputs.empty(),
         "There are outputs that should still reference this node. Thus this node shouldn't get deleted");

  // We're in the destructor, thus we must make sure we're not calling any virtual methods - so we're doing the removal
  // directly instead of calling set_left_input/right_input(nullptr)
  if (_inputs[0]) {
    _inputs[0]->_remove_output_pointer(*this);
  }

  if (_inputs[1]) {
    _inputs[1]->_remove_output_pointer(*this);
  }
}

size_t AbstractLQPNode::hash() const {
  size_t hash{0};

  visit_lqp(shared_from_this(), [&hash](const auto& node) {
    if (node) {
      for (const auto& expression : node->node_expressions) {
        boost::hash_combine(hash, expression->hash());
      }
      boost::hash_combine(hash, node->type);
      boost::hash_combine(hash, node->_on_shallow_hash());
      return LQPVisitation::VisitInputs;
    }

    return LQPVisitation::DoNotVisitInputs;
  });

  return hash;
}

size_t AbstractLQPNode::_on_shallow_hash() const {
  return 0;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::left_input() const {
  return _inputs[0];
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::right_input() const {
  return _inputs[1];
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::input(LQPInputSide side) const {
  const auto input_index = static_cast<int>(side);
  return _inputs[input_index];
}

void AbstractLQPNode::set_left_input(const std::shared_ptr<AbstractLQPNode>& left) {
  set_input(LQPInputSide::Left, left);
}

void AbstractLQPNode::set_right_input(const std::shared_ptr<AbstractLQPNode>& right) {
  DebugAssert(right == nullptr || type == LQPNodeType::Join || type == LQPNodeType::Union ||
                  type == LQPNodeType::Update || type == LQPNodeType::Intersect || type == LQPNodeType::Except ||
                  type == LQPNodeType::ChangeMetaTable || type == LQPNodeType::Mock,
              "This node type does not accept a right input");
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
  }

  if (output->_inputs[1].get() == this) {
    return LQPInputSide::Right;
  }

  Fail("Specified output node is not actually an output node of this node.");
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

// clang-tidy wants this to be const. Technically, it could be, but as this node will be modified via set_input, it is
// syntactically incorrect.
void AbstractLQPNode::remove_output(const std::shared_ptr<AbstractLQPNode>& output) {  // NOLINT
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

  const auto output_relation_count = output_relations.size();
  for (auto output_idx = size_t{0}; output_idx < output_relation_count; ++output_idx) {
    output_relations[output_idx] = LQPOutputRelation{outputs[output_idx], input_sides[output_idx]};
  }

  return output_relations;
}

size_t AbstractLQPNode::output_count() const {
  return _outputs.size();
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy(LQPNodeMapping node_mapping) const {
  const auto copy = _deep_copy_impl(node_mapping);

  // StoredTableNodes can store references to PredicateNodes as prunable subquery predicates (see get_table.hpp for
  // details). We must assign the copies of these PredicateNodes after copying the entire LQP (see
  // map_prunable_subquery_predicates.hpp).
  map_prunable_subquery_predicates(node_mapping);

  return copy;
}

bool AbstractLQPNode::shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  if (type != rhs.type) {
    return false;
  }
  return _on_shallow_equals(rhs, node_mapping);
}

std::vector<std::shared_ptr<AbstractExpression>> AbstractLQPNode::output_expressions() const {
  Assert(left_input() && !right_input(),
         "Can only forward input expressions iff there is a left input and no right input");
  return left_input()->output_expressions();
}

std::optional<ColumnID> AbstractLQPNode::find_column_id(const AbstractExpression& expression) const {
  const auto& expressions_of_output = output_expressions();
  return find_expression_idx(expression, expressions_of_output);
}

ColumnID AbstractLQPNode::get_column_id(const AbstractExpression& expression) const {
  const auto column_id = find_column_id(expression);
  Assert(column_id, "This node has no column '" + expression.as_column_name() + "'.");
  return *column_id;
}

bool AbstractLQPNode::has_output_expressions(const ExpressionUnorderedSet& expressions) const {
  const auto& output_expressions = this->output_expressions();
  return contains_all_expressions(expressions, output_expressions);
}

bool AbstractLQPNode::is_column_nullable(const ColumnID column_id) const {
  // Default behaviour: Forward from input
  Assert(left_input() && !right_input(),
         "Can forward nullability from input iff there is a left input and no right input");
  return left_input()->is_column_nullable(column_id);
}

bool AbstractLQPNode::has_matching_ucc(const ExpressionUnorderedSet& expressions) const {
  DebugAssert(!expressions.empty(), "Invalid input. Set of expressions should not be empty.");
  DebugAssert(has_output_expressions(expressions),
              "The given expressions are not a subset of the LQP's output expressions.");

  const auto& unique_column_combinations = this->unique_column_combinations();
  if (unique_column_combinations.empty()) {
    return false;
  }

  return contains_matching_unique_column_combination(unique_column_combinations, expressions);
}

FunctionalDependencies AbstractLQPNode::functional_dependencies() const {
  // (1) Gather non-trivial FDs and perform sanity checks.
  const auto& non_trivial_fds = non_trivial_functional_dependencies();
  if constexpr (HYRISE_DEBUG) {
    auto fds = FunctionalDependencies{};
    const auto& output_expressions = this->output_expressions();
    const auto& output_expressions_set = ExpressionUnorderedSet{output_expressions.cbegin(), output_expressions.cend()};

    for (const auto& fd : non_trivial_fds) {
      auto [_, inserted] = fds.insert(fd);
      Assert(inserted, "FDs with the same set of determinant expressions should be merged.");

      for (const auto& fd_determinant_expression : fd.determinants) {
        Assert(output_expressions_set.contains(fd_determinant_expression),
               "Expected FD's determinant expressions to be a subset of the node's output expressions.");
        Assert(!is_column_nullable(get_column_id(*fd_determinant_expression)),
               "Expected FD's determinant expressions to be non-nullable.");
      }
      Assert(std::all_of(fd.dependents.cbegin(), fd.dependents.cend(),
                         [&output_expressions_set](const auto& fd_dependent_expression) {
                           return output_expressions_set.contains(fd_dependent_expression);
                         }),
             "Expected the FD's dependent expressions to be a subset of the node's output expressions.");
    }
  }

  // (2) Derive trivial FDs from the node's unique column combinations.
  const auto& unique_column_combinations = this->unique_column_combinations();
  // Early exit if there are no UCCs.
  if (unique_column_combinations.empty()) {
    return non_trivial_fds;
  }

  const auto& trivial_fds = fds_from_unique_column_combinations(shared_from_this(), unique_column_combinations);

  // (3) Merge and return FDs.
  return union_fds(non_trivial_fds, trivial_fds);
}

FunctionalDependencies AbstractLQPNode::non_trivial_functional_dependencies() const {
  if (left_input()) {
    Assert(!right_input(), "Expected single input node for implicit FD forwarding. Please override this function.");
    return left_input()->non_trivial_functional_dependencies();
  }

  // For instance, StoredTableNode or StaticTableNode cannot provide any non-trivial FDs.
  return {};
}

bool AbstractLQPNode::operator==(const AbstractLQPNode& rhs) const {
  if (this == &rhs) {
    return true;
  }
  return !lqp_find_subplan_mismatch(shared_from_this(), rhs.shared_from_this());
}

bool AbstractLQPNode::operator!=(const AbstractLQPNode& rhs) const {
  return !operator==(rhs);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_deep_copy_impl(LQPNodeMapping& node_mapping) const {
  auto copied_left_input = std::shared_ptr<AbstractLQPNode>{};
  auto copied_right_input = std::shared_ptr<AbstractLQPNode>{};

  if (left_input()) {
    copied_left_input = left_input()->_deep_copy_impl(node_mapping);
  }

  if (right_input()) {
    copied_right_input = right_input()->_deep_copy_impl(node_mapping);
  }

  auto copy = _shallow_copy(node_mapping);
  copy->set_left_input(copied_left_input);
  copy->set_right_input(copied_right_input);

  return copy;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto node_mapping_iter = node_mapping.find(shared_from_this());

  // Handle diamond shapes in the LQP; don't copy nodes twice
  if (node_mapping_iter != node_mapping.end()) {
    return node_mapping_iter->second;
  }

  auto shallow_copy = _on_shallow_copy(node_mapping);
  shallow_copy->comment = comment;
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

UniqueColumnCombinations AbstractLQPNode::_forward_left_unique_column_combinations() const {
  Assert(left_input(), "Cannot forward unique column combinations without an input node.");
  const auto& input_unique_column_combinations = left_input()->unique_column_combinations();

  if constexpr (HYRISE_DEBUG) {
    // Check whether output expressions are missing
    for (const auto& ucc : input_unique_column_combinations) {
      Assert(has_output_expressions(ucc.expressions),
             "Forwarding of UCC is illegal because node misses output expressions.");
    }
  }
  return input_unique_column_combinations;
}

AbstractExpression::DescriptionMode AbstractLQPNode::_expression_description_mode(const DescriptionMode mode) {
  switch (mode) {
    case DescriptionMode::Short:
      return AbstractExpression::DescriptionMode::ColumnName;
    case DescriptionMode::Detailed:
      return AbstractExpression::DescriptionMode::Detailed;
  }
  Fail("Unhandled DescriptionMode");
}

std::ostream& operator<<(std::ostream& stream, const AbstractLQPNode& node) {
  // Recursively collect all LQPs in LQPSubqueryExpressions (and any anywhere within those) in this LQP into a list and
  // then print them
  auto lqps = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_lqps_in_plan(node, lqps);

  const auto output_lqp_to_stream = [&](const auto& root) {
    const auto get_inputs_fn = [](const auto& node2) {
      std::vector<std::shared_ptr<const AbstractLQPNode>> inputs;
      if (node2->left_input()) {
        inputs.emplace_back(node2->left_input());
      }

      if (node2->right_input()) {
        inputs.emplace_back(node2->right_input());
      }

      return inputs;
    };

    const auto node_print_fn = [](const auto& node2, auto& stream2) {
      stream2 << node2->description(AbstractLQPNode::DescriptionMode::Detailed);
      if (!node2->comment.empty()) {
        stream2 << " (" << node2->comment << ")";
      }
      stream2 << " @ " << node2;
    };

    print_directed_acyclic_graph<const AbstractLQPNode>(root.shared_from_this(), get_inputs_fn, node_print_fn, stream);
  };

  output_lqp_to_stream(node);

  if (lqps.empty()) {
    return stream;
  }

  stream << "-------- Subqueries ---------" << std::endl;

  for (const auto& lqp : lqps) {
    stream << lqp.get() << ": " << std::endl;
    output_lqp_to_stream(*lqp);
    stream << std::endl;
  }

  return stream;
}

}  // namespace hyrise
