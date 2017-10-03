#include "projection_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"

#include "common.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<Expression>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Projection), _column_expressions(column_expressions) {}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (const auto& column : output_column_names()) {
    desc << " " << column;
  }

  return desc.str();
}

const std::vector<std::shared_ptr<Expression>>& ProjectionNode::column_expressions() const {
  return _column_expressions;
}

void ProjectionNode::_on_child_changed() {
  /**
   * Populates `_output_column_names` and `_output_column_id_to_input_column_id`.
   */

  _output_column_names.clear();
  _output_column_id_to_input_column_id.clear();

  _output_column_names.reserve(_column_expressions.size());
  _output_column_id_to_input_column_id.reserve(_column_expressions.size());

  for (const auto& expression : _column_expressions) {
    // If the expression defines an alias, use it as the output column name.
    // If it does not, we have to handle it differently, depending on the type of the expression.
    if (expression->alias()) {
      _output_column_names.emplace_back(*expression->alias());
    }

    if (expression->type() == ExpressionType::Column) {
      DebugAssert(left_child(), "ProjectionNode needs a child.");

      _output_column_id_to_input_column_id.emplace_back(expression->column_id());

      if (!expression->alias()) {
        const auto& column_name = left_child()->output_column_names()[expression->column_id()];
        _output_column_names.emplace_back(column_name);
      }

    } else if (expression->type() == ExpressionType::Literal || expression->is_arithmetic_operator()) {
      _output_column_id_to_input_column_id.emplace_back(INVALID_COLUMN_ID);

      if (!expression->alias()) {
        _output_column_names.emplace_back(expression->to_string(left_child()->output_column_names()));
      }

    } else {
      Fail("Only column references, arithmetic expressions, and literals supported for now.");
    }
  }
}

const std::vector<ColumnID>& ProjectionNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
}

const std::vector<std::string>& ProjectionNode::output_column_names() const { return _output_column_names; }

optional<ColumnID> ProjectionNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  /**
   * The result variable. We make sure the optional is only set once to detect ambiguity in column
   * references.
   */
  optional<ColumnID> result_column_id;

  /**
   * Look for named_column_reference in the input node, if it exists there, check whether one of this node's
   * _column_expressions match the found column_id.
   * The fact that the input node contains the named_column_reference doesn't necessarily mean that it is the column
   * we're looking for. E.g: we're looking for column "a" and "a" exists in the previous node, but is NOT projected by
   * the projection it might still be an ALIAS of the projection.
   */
  const auto child_column_id = left_child()->find_column_id_by_named_column_reference(named_column_reference);

  for (ColumnID column_id{0}; column_id < output_column_names().size(); column_id++) {
    const auto& column_expression = _column_expressions[column_id];

    /**
     * Check whether column_identifier_name is _NOT_ ALIASed and projected by this node, e.g. we're looking for
     * `t1.a` in `SELECT t1.a, t1.b AS c FROM ...
     */
    if (child_column_id && column_expression->type() == ExpressionType::Column &&
        column_expression->column_id() == *child_column_id && !column_expression->alias()) {
      Assert(!result_column_id, "Column name " + named_column_reference.column_name + " is ambiguous.");
      result_column_id = column_id;
      continue;
    }

    /**
     * If the named_column_reference we're looking for doesn't refer to a table, i.e. only the
     * ColumnIdentifierName::column_name is set, then it is possible that ColumnIdentifierName::column_name refers to
     * either one of the Projection's ALIASes or column names generated based on arithmetic expressions (i.e. 5+3 ->
     * "5+3").
     */
    if (!named_column_reference.table_name) {
      if (column_expression->alias()) {
        // Check whether `named_column_reference` is the ALIAS of a column, e.g. `a AS some_a` or `a+b AS sum_ab`
        if (*column_expression->alias() == named_column_reference.column_name) {
          Assert(!result_column_id, "Column name " + named_column_reference.column_name + " is ambiguous.");
          result_column_id = column_id;
          continue;
        }
      } else {
        // Check whether `named_column_reference` is the generated name of a column, e.g. `a+b` without ALIAS
        if (column_expression->to_string(left_child()->output_column_names()) == named_column_reference.column_name) {
          Assert(!result_column_id, "Column name " + named_column_reference.column_name + " is ambiguous.");
          result_column_id = column_id;
          continue;
        }
      }
    }
  }

  return result_column_id;
}

std::vector<ColumnID> ProjectionNode::get_output_column_ids_for_table(const std::string& table_name) const {
  DebugAssert(left_child(), "ProjectionNode needs a child.");

  if (!left_child()->knows_table(table_name)) {
    return {};
  }

  const auto input_column_ids_for_table = left_child()->get_output_column_ids_for_table(table_name);

  std::vector<ColumnID> output_column_ids_for_table;

  for (const auto input_column_id : input_column_ids_for_table) {
    const auto iter = std::find(_output_column_id_to_input_column_id.begin(),
                                _output_column_id_to_input_column_id.end(), input_column_id);

    if (iter != _output_column_id_to_input_column_id.end()) {
      const auto column_id =
          ColumnID{static_cast<ColumnID::base_type>(std::distance(_output_column_id_to_input_column_id.begin(), iter))};
      output_column_ids_for_table.emplace_back(column_id);
    }
  }

  return output_column_ids_for_table;
}

}  // namespace opossum
