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

  // Adopt single column unique constraints
  for(const auto& constraint : input_lqp_constraints) {
    if(constraint.is_primary_key == IsPrimaryKey::No && constraint.columns.size() == 1) {
      const auto constraint_column_expr = left_input()->column_expressions().at(constraint.columns[0]);

      // Check applicability of constraint for every node_expressions
      for(ColumnID projection_c_id{0}; projection_c_id < node_expressions.size(); projection_c_id++) {
        const auto projection_expr = node_expressions[projection_c_id];

        switch (projection_expr->type) {
        case ExpressionType::LQPColumn:
          const auto column_reference = std::dynamic_pointer_cast<LQPColumnExpression>(projection_expr)->column_reference;
          if(constraint.columns[0] ==  column_reference.original_column_id()) {
            projection_lqp_constraints->push_back(UniqueConstraintDefinition{std::vector<ColumnID>{ColumnID{projection_c_id}}, IsPrimaryKey::No});
          }
          break;
        case ExpressionType::Arithmetic:
          break;
        default:
          break;
//          Aggregate,
//          Cast,
//          Case,
//          CorrelatedParameter,
//          PQPColumn,
//          Exists,
//          Extract,
//          Function,
//          List,
//          Logical,
//          Placeholder,
//          Predicate,
//          PQPSubquery,
//          LQPSubquery,
//          UnaryMinus,
//          Value
        }
      }
    }
  }
  return projection_lqp_constraints;
}

bool constraint_applicable(AbstractExpression projection_expr) {

}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping));
}

bool ProjectionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).node_expressions;
  return expressions_equal_to_expressions_in_different_lqp(node_expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
