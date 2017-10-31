#include "projection_node.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<Expression>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Projection), _column_expressions(column_expressions) {}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "[Projection] ";

  std::vector<std::string> verbose_column_names;
  if (left_child()) {
    verbose_column_names = left_child()->get_verbose_column_names();
  }

  for (size_t column_idx = 0; column_idx < _column_expressions.size(); ++column_idx) {
    desc << _column_expressions[column_idx]->to_string(verbose_column_names);
    if (column_idx + 1 < _column_expressions.size()) {
      desc << ", ";
    }
  }

  return desc.str();
}

const std::vector<std::shared_ptr<Expression>>& ProjectionNode::column_expressions() const {
  return _column_expressions;
}

void ProjectionNode::_on_child_changed() {
  DebugAssert(!right_child(), "Projection can't have a right child");

  _output_column_names.reset();
}

const std::vector<ColumnID>& ProjectionNode::output_column_ids_to_input_column_ids() const {
  if (!_output_column_ids_to_input_column_ids) {
    _update_output();
  }

  return *_output_column_ids_to_input_column_ids;
}

const std::vector<std::string>& ProjectionNode::output_column_names() const {
  if (!_output_column_names) {
    _update_output();
  }
  return *_output_column_names;
}

void ProjectionNode::map_column_ids(const ColumnIDMapping& column_id_mapping, ASTChildSide caller_child_side) {
  DebugAssert(left_child(),
              "Input needs to be set to perform this operation. Mostly because we can't validate the size of "
              "column_id_mapping otherwise.");
  DebugAssert(column_id_mapping.size() == left_child()->output_column_count(), "Invalid column_id_mapping");

  for (const auto& column_expression : _column_expressions) {
    column_expression->map_column_ids(column_id_mapping);
  }
}

std::optional<ColumnID> ProjectionNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  /**
   * The result variable. We make sure the optional is only set once to detect ambiguity in column
   * references.
   */
  std::optional<ColumnID> result_column_id;

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  }

  /**
   * Look for named_column_reference in the input node, if it exists there, check whether one of this node's
   * _column_expressions match the found column_id.
   * The fact that the input node contains the named_column_reference doesn't necessarily mean that it is the column
   * we're looking for. E.g: we're looking for column "a" and "a" exists in the previous node, but is NOT projected by
   * the projection it might still be an ALIAS of the projection.
   */
  const auto child_column_id =
      left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);

  for (ColumnID column_id{0}; column_id < output_column_names().size(); column_id++) {
    const auto& column_expression = _column_expressions[column_id];

    /**
     * Check whether column_identifier_name is _NOT_ ALIASed and projected by this node, e.g. we're looking for
     * `t1.a` in `SELECT t1.a, t1.b AS c FROM ...
     */
    if (child_column_id && column_expression->type() == ExpressionType::Column &&
        column_expression->column_id() == *child_column_id && !column_expression->alias()) {
      Assert(!result_column_id,
             "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");
      result_column_id = column_id;
      continue;
    }

    /**
     * If the named_column_reference we're looking for doesn't refer to a table, i.e. only the
     * ColumnIdentifierName::column_name is set, then it is possible that ColumnIdentifierName::column_name refers to
     * either one of the Projection's ALIASes or column names generated based on arithmetic expressions (i.e. 5+3 ->
     * "5+3").
     */
    if (!named_column_reference_without_local_alias->table_name) {
      if (column_expression->alias()) {
        // Check whether `named_column_reference` is the ALIAS of a column, e.g. `a AS some_a` or `a+b AS sum_ab`
        if (*column_expression->alias() == named_column_reference_without_local_alias->column_name) {
          Assert(!result_column_id,
                 "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");
          result_column_id = column_id;
          continue;
        }
      } else {
        // Check whether `named_column_reference` is the generated name of a column, e.g. `a+b` without ALIAS
        if (column_expression->to_string(left_child()->output_column_names()) ==
            named_column_reference_without_local_alias->column_name) {
          Assert(!result_column_id,
                 "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");
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

  if (!knows_table(table_name)) {
    return {};
  }

  if (_table_alias && *_table_alias == table_name) {
    return get_output_column_ids();
  }

  const auto input_column_ids_for_table = left_child()->get_output_column_ids_for_table(table_name);

  std::vector<ColumnID> output_column_ids_for_table;

  const auto& output_column_ids_to_input_column_ids = this->output_column_ids_to_input_column_ids();

  for (const auto input_column_id : input_column_ids_for_table) {
    const auto iter = std::find(output_column_ids_to_input_column_ids.begin(),
                                output_column_ids_to_input_column_ids.end(), input_column_id);

    if (iter != output_column_ids_to_input_column_ids.end()) {
      const auto column_id = ColumnID{
          static_cast<ColumnID::base_type>(std::distance(output_column_ids_to_input_column_ids.begin(), iter))};
      output_column_ids_for_table.emplace_back(column_id);
    }
  }

  return output_column_ids_for_table;
}

std::string ProjectionNode::get_verbose_column_name(ColumnID column_id) const {
  DebugAssert(left_child(), "Need input to generate name");
  DebugAssert(column_id < _column_expressions.size(), "ColumnID out of range");

  const auto& column_expression = _column_expressions[column_id];

  if (column_expression->alias()) {
    return *column_expression->alias();
  }

  if (left_child()) {
    return column_expression->to_string(left_child()->output_column_names());
  } else {
    return column_expression->to_string();
  }
}

void ProjectionNode::_update_output() const {
  /**
   * The output (column names and output-to-input mapping) of this node gets cleared whenever a child changed and is
   * re-computed on request. This allows ASTs to be in temporary invalid states (e.g. no left child in Join) and thus
   * allows easier manipulation in the optimizer.
   */

  DebugAssert(!_output_column_ids_to_input_column_ids, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(!_output_column_names, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(left_child(), "Can't set output without input");

  _output_column_names.emplace();
  _output_column_names->reserve(_column_expressions.size());

  _output_column_ids_to_input_column_ids.emplace();
  _output_column_ids_to_input_column_ids->reserve(_column_expressions.size());

  for (const auto& expression : _column_expressions) {
    // If the expression defines an alias, use it as the output column name.
    // If it does not, we have to handle it differently, depending on the type of the expression.
    if (expression->alias()) {
      _output_column_names->emplace_back(*expression->alias());
    }

    if (expression->type() == ExpressionType::Column) {
      DebugAssert(left_child(), "ProjectionNode needs a child.");

      _output_column_ids_to_input_column_ids->emplace_back(expression->column_id());

      if (!expression->alias()) {
        Assert(expression->column_id() < left_child()->output_column_names().size(), "ColumnID out of range");
        const auto& column_name = left_child()->output_column_names()[expression->column_id()];
        _output_column_names->emplace_back(column_name);
      }

    } else if (expression->type() == ExpressionType::Literal || expression->is_arithmetic_operator()) {
      _output_column_ids_to_input_column_ids->emplace_back(INVALID_COLUMN_ID);

      if (!expression->alias()) {
        _output_column_names->emplace_back(expression->to_string(left_child()->output_column_names()));
      }

    } else {
      Fail("Only column references, arithmetic expressions, and literals supported for now.");
    }
  }
}

}  // namespace opossum
