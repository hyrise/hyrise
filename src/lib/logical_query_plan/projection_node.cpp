#include "projection_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions):
  AbstractLQPNode(LQPNodeType::Projection), expressions(expressions) {}

bool ProjectionNode::shallow_equals(const AbstractLQPNode& rhs) const {
  return false;
}

const std::vector<std::shared_ptr<AbstractExpression>>& ProjectionNode::output_column_expressions() const {
  return expressions;
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::deep_copy() const {
  Fail("Hard");
}

}  // namespace opossum


//#include "projection_node.hpp"
//
//#include <algorithm>
//#include <memory>
//#include <optional>
//#include <sstream>
//#include <string>
//#include <vector>
//
//#include "types.hpp"
//#include "expression/lqp_column_expression.hpp"
//#include "expression/lqp_select_expression.hpp"
//
//namespace opossum {
//
//std::shared_ptr<ProjectionNode> ProjectionNode::make_pass_through(const std::shared_ptr<AbstractLQPNode>& input) {
//  std::vector<PlanColumnDefinition> column_definitions;
//  column_definitions.reserve(input->output_column_count());
//  for (const auto& column_reference : input->output_column_references()) {
//    column_definitions.emplace_back(std::make_shared<LQPColumnReference>(column_reference));
//  }
//
//  const auto projection_node = ProjectionNode::make(column_definitions, input);
//  return projection_node;
//}
//
//ProjectionNode::ProjectionNode(const std::vector<PlanColumnDefinition>& column_definitions)
//    : AbstractLQPNode(LQPNodeType::Projection), _column_definitions(column_definitions) {}
//
//std::string ProjectionNode::description() const {
//  std::ostringstream desc;
//
//  desc << "[Projection] ";
//
//  for (size_t column_idx = 0; column_idx < _column_definitions.size(); ++column_idx) {
//    desc << _column_definitions[column_idx].description();
//    if (column_idx + 1 < _column_definitions.size()) {
//      desc << ", ";
//    }
//  }
//
//  return desc.str();
//}
//
//std::shared_ptr<AbstractLQPNode> ProjectionNode::_deep_copy_impl(
//    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
//    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
//  Assert(left_input() && copied_left_input, "Can't deep copy without input to adjust ColumnReferences");
//
//  std::vector<std::shared_ptr<PlanColumnDefinition>> column_definitions;
//  column_definitions.reserve(_column_definitions.size());
//  for (const auto& column_definition : _column_definitions) {
//    auto expression = column_definition.expression->deep_copy();
//    adapt_expression_to_different_lqp(*expression, *left_input(), *copied_left_input)
//    column_definitions.emplace_back(expression, column_definition.alias);
//  }
//
//  return ProjectionNode::make(column_definitions);
//}
//
//const std::vector<PlanColumnDefinition>& ProjectionNode::column_definitions() const {
//  return _column_definitions;
//}
//
//void ProjectionNode::_on_input_changed() {
//  DebugAssert(!right_input(), "Projection can't have a right input");
//
//  _output_column_names.reset();
//}
//
//const std::vector<LQPColumnReference>& ProjectionNode::output_column_references() const {
//  if (!_output_column_references) {
//    _update_output();
//  }
//
//  return *_output_column_references;
//}
//
//const std::vector<std::string>& ProjectionNode::output_column_names() const {
//  if (!_output_column_names) {
//    _update_output();
//  }
//  return *_output_column_names;
//}
//
//const std::vector<std::shared_ptr<AbstractExpression>>& ProjectionNode::output_column_expressions() const {
//  if (!_output_column_expressions) {
//    _output_column_expressions->emplace();
//    _output_column_expressions->reserve(output_column_count());
//    for (const auto& column_definition : _column_definitions) {
//      _output_column_expressions->emplace_back(column_definition.expression);
//    }
//  }
//  return *_output_column_expressions;
//}
//
//std::string ProjectionNode::get_verbose_column_name(ColumnID column_id) const {
//  DebugAssert(left_input(), "Need input to generate name");
//  DebugAssert(column_id < _output_column_expressions.size(), "ColumnID out of range");
//
//  const auto& column_expression = _output_column_expressions[column_id].expression;
//
//  if (column_expression->alias()) {
//    return *column_expression->alias();
//  }
//
//  return column_expression->description();
//}
//
//bool ProjectionNode::shallow_equals(const AbstractLQPNode& rhs) const {
//  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
//  const auto& projection_node = dynamic_cast<const ProjectionNode&>(rhs);
//
//  Assert(left_input() && rhs.left_input(), "Can't compare column references without inputs");
//  return _equals(*left_input(), _column_definitions, *projection_node.left_input(),
//                 projection_node._column_definitions);
//}
//
//void ProjectionNode::_update_output() const {
//  /**
//   * The output (column names and output-to-input mapping) of this node gets cleared whenever an input changed and is
//   * re-computed on request. This allows LQPs to be in temporary invalid states (e.g. no left input in Join) and thus
//   * allows easier manipulation in the optimizer.
//   */
//
//  DebugAssert(!_output_column_names, "No need to update, _update_output() shouldn't get called.");
//  DebugAssert(!_output_column_names, "No need to update, _update_output() shouldn't get called.");
//  DebugAssert(left_input(), "Can't set output without input");
//
//  _output_column_names.emplace();
//  _output_column_names->reserve(_column_definitions.size());
//
//  _output_column_references.emplace();
//  _output_column_references->reserve(_column_definitions.size());
//
//  auto column_id = ColumnID{0};
//  for (const auto& column_definition : _column_definitions) {
//    // If the expression defines an alias, use it as the output column name.
//    // If it does not, we have to handle it differently, depending on the type of the expression.
//    if (column_definition.alias) {
//      _output_column_names->emplace_back(*column_definition.alias);
//    } else {
//      _output_column_names->emplace_back(column_definition.expression->as_column_name());
//    }
//
//    const auto& expression = column_definition.expression;
//
//    if (expression->type == ExpressionType::Column) {
//      DebugAssert(left_input(), "ProjectionNode needs a input.");
//
//      const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
//      DebugAssert(column_expression->column_reference, "Can't use unresolved ColumnExpression here");
//
//      _output_column_references->emplace_back(column_expression->column_reference);
//    } else if (expression->type == ExpressionType::Select) {
//      auto select_expression = std::static_pointer_cast<LQPSelectExpression>(expression);
//      auto node = select_expression->lqp;
//
//      _output_column_references->emplace_back(node, ColumnID(0));
//    } else {
//      _output_column_references->emplace_back(shared_from_this(), column_id);
//    }
//
//    column_id++;
//  }
//}
//
//}  // namespace opossum
