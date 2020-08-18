#include "projection_node.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "lqp_utils.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractLQPNode(LQPNodeType::Projection, expressions) {}

std::string ProjectionNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;

  stream << "[Projection] " << expression_descriptions(node_expressions, expression_mode);

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> ProjectionNode::output_expressions() const { return node_expressions; }

bool ProjectionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < node_expressions.size(), "ColumnID out of range");
  Assert(left_input(), "Need left input to determine nullability");
  return node_expressions[column_id]->is_nullable_on_lqp(*left_input());
}

std::shared_ptr<LQPUniqueConstraints> ProjectionNode::unique_constraints() const {
  auto unique_constraints = std::make_shared<LQPUniqueConstraints>();
  unique_constraints->reserve(node_expressions.size());

  // Forward unique constraints, if applicable
  const auto& input_unique_constraints = left_input()->unique_constraints();

  for (const auto& input_unique_constraint : *input_unique_constraints) {
    if (!has_output_expressions(input_unique_constraint.expressions)) {
      continue;
      /**
       * Future Work:
       * Our implementation does not exploit all opportunities yet.
       * As the next step, we could check for derived output expressions that preserve uniqueness, for example,
       * the expression 'column + 1'.
       * Instead of discarding a unique constraint for 'column', we could create and output a new one for 'column + 1'.
       */
    }
    unique_constraints->emplace_back(input_unique_constraint);
  }

  return unique_constraints;
}

std::vector<FunctionalDependency> ProjectionNode::non_trivial_functional_dependencies() const {
  auto non_trivial_fds = left_input()->non_trivial_functional_dependencies();

  // Currently, we remove non-trivial FDs whose expressions are no longer part of the node's output expressions.
  remove_invalid_fds(shared_from_this(), non_trivial_fds);

  /**
   * Future Work: By analyzing the output expressions in more depth, we can save some of the input FDs. For example:
   *               - StoredTableNode with the columns a, b, c and the following FD:
   *                 {a} -> {b}
   *               - ProjectionNode with the following output expressions:
   *                 a, (b + 1)
   *
   *              The current implementation discards the above FD. Instead, we could save it via the following
   *              reformulation: {a} -> {b + 1}
   */

  return non_trivial_fds;
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping));
}

bool ProjectionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).node_expressions;
  return expressions_equal_to_expressions_in_different_lqp(node_expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
