#include "projection_node.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
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

const std::shared_ptr<LQPUniqueConstraints> ProjectionNode::unique_constraints() const {
  auto unique_constraints = std::make_shared<LQPUniqueConstraints>();
  unique_constraints->reserve(node_expressions.size());

  auto input_unique_constraints = left_input()->unique_constraints();

  // Check each input constraint for applicability in this projection node
  const auto& expressions = output_expressions();
  const auto expressions_set = ExpressionUnorderedSet{expressions.cbegin(), expressions.cend()};

  for (const auto& input_unique_constraint : *input_unique_constraints) {
    // Check whether column expressions have been filtered out with this node.
    bool found_all_column_expressions = std::all_of(
        input_unique_constraint.column_expressions.cbegin(), input_unique_constraint.column_expressions.cend(),
        [&](const std::shared_ptr<AbstractExpression>& constraint_column_expr) {
          return expressions_set.contains(constraint_column_expr);
        });

    if (found_all_column_expressions) {
      unique_constraints->insert(input_unique_constraint);
    }  // else { save constraint for the next block - derived constraints }
  }

  // TODO(anyone) Very basic check above. In the future, we also might want to look for
  //  derived column expressions, like 'column + 1', that preserve uniqueness.
  //  However, in case of derived column expressions we also have to create new, derived constraints.
  // { ... }

  return unique_constraints;
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping));
}

bool ProjectionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).node_expressions;
  return expressions_equal_to_expressions_in_different_lqp(node_expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
