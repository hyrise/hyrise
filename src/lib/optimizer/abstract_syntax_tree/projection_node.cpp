#include "projection_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression/expression_node.hpp"

#include "common.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Projection), _column_expressions(column_expressions) {}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (const auto& column : output_column_names()) {
    desc << " " << column;
  }

  return desc.str();
}

const std::vector<std::shared_ptr<ExpressionNode>>& ProjectionNode::column_expressions() const {
  return _column_expressions;
}

void ProjectionNode::_set_output_information() const {
  /**
   * Populates `_output_column_names` and `_output_column_ids`.
   * This cannot be done in the constructor because children have to be set to resolve column names.
   */
  DebugAssert(!!left_child(), "ProjectionNode needs a child.");

  // Find the maximum ColumnID of the columns that are projected.
  // This is required to generate new ColumnIDs in case there are expressions that are not ColumnReferences,
  // for which the Projection creates new columns.
  auto iter =
      std::max_element(_column_expressions.cbegin(), _column_expressions.cend(),
                       [](const std::shared_ptr<ExpressionNode>& left, const std::shared_ptr<ExpressionNode>& right) {
                         return left->column_id() < right->column_id();
                       });
  auto current_column_id = static_cast<uint16_t>((*iter)->column_id());

  for (const auto& expression : _column_expressions) {
    // If the expression defines an alias, use it as the output column name.
    // If it does not, we have to handle it differently, depending on the type of the expression.
    if (expression->alias()) {
      _output_column_names.emplace_back(*expression->alias());
    }

    if (expression->type() == ExpressionType::ColumnReference) {
      _output_column_ids.emplace_back(expression->column_id());

      if (!expression->alias()) {
        const auto& column_name = left_child()->get_column_name_for_column_id(expression->column_id());
        _output_column_names.emplace_back(column_name);
      }
    } else if (expression->type() == ExpressionType::Literal || expression->is_arithmetic_operator()) {
      // Generate a new ColumnID.
      current_column_id++;
      _output_column_ids.emplace_back(current_column_id);

      if (!expression->alias()) {
        _output_column_names.emplace_back(expression->to_string());
      }
    } else {
      Fail("Only column references, arithmetic expressions, and literals supported for now.");
    }
  }
}

std::vector<ColumnID> ProjectionNode::output_column_ids() const {
  if (_output_column_ids.empty()) {
    _set_output_information();
  }

  return _output_column_ids;
}

std::vector<std::string> ProjectionNode::output_column_names() const {
  if (_output_column_names.empty()) {
    _set_output_information();
  }

  return _output_column_names;
}

optional<ColumnID> ProjectionNode::find_column_id_for_column_identifier(
    const ColumnIdentifier& column_identifier) const {
  optional<ColumnID> column_id;

  for (uint16_t column_idx = 0; column_idx < output_column_names().size(); column_idx++) {
    const auto& name = output_column_names()[column_idx];
    if (column_identifier.column_name == name) {
      Assert(!column_id, "Column name " + column_identifier.column_name + " is ambiguous.");
      column_id = output_column_ids()[column_idx];
    }
  }

  return column_id;
}

}  // namespace opossum
