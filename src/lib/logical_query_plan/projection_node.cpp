#include "projection_node.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractLQPNode(LQPNodeType::Projection, expressions) {}

std::string ProjectionNode::description() const {
  std::stringstream stream;

  stream << "[Projection] " << expression_column_names(node_expressions);

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& ProjectionNode::column_expressions() const {
  return node_expressions;
}

bool ProjectionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < node_expressions.size(), "ColumnID out of range");
  Assert(left_input(), "Need left input to determine nullability");
  return node_expressions[column_id]->is_nullable_on_lqp(*left_input());
}

const std::shared_ptr<ExpressionsConstraintDefinitions> ProjectionNode::get_constraints() const {
  auto input_lqp_constraints = *left_input()->get_constraints();
  auto projection_lqp_constraints = std::make_shared<ExpressionsConstraintDefinitions>();
  projection_lqp_constraints->reserve(node_expressions.size());

  // Check each input constraint for applicability in this projection node
  for(const auto& constraint : input_lqp_constraints) {

    // Check whether column expressions have been filtered out with this node.
    bool found_all_column_expressions = std::all_of(constraint.column_expressions.cbegin(), constraint.column_expressions.cend(),
       [&](const std::shared_ptr<AbstractExpression>& constraint_column_expr) {

         const auto matching_prj_column_expr = std::find_if(node_expressions.cbegin(), node_expressions.cend(),
                                                            [&](const std::shared_ptr<AbstractExpression>& node_expr) {
                                                              if (*node_expr == *constraint_column_expr)
                                                                return true;
                                                              return false;
                                                            });
         return matching_prj_column_expr != node_expressions.cend();
    });

    if(found_all_column_expressions) {
      projection_lqp_constraints->push_back(constraint);
    } // else { save constraint for the next block - derived constraints }
  }

  // TODO(anyone) Very basic equality check here. In the future, we might want to look for derived expressions,
  //  like 'column + 1', that preserve uniqueness.
  //  However, we can't pass constraints then, we have to create derived constraints.
  // { ... }

  return projection_lqp_constraints;
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping));
}

bool ProjectionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).node_expressions;
  return expressions_equal_to_expressions_in_different_lqp(node_expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
