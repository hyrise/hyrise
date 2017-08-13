#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "projection.hpp"
#include "storage/reference_column.hpp"

#include "resolve_type.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const std::vector<std::shared_ptr<ExpressionNode>> & column_expressions)
    : AbstractReadOnlyOperator(in), _column_expressions(column_expressions) {}

const std::string Projection::name() const { return "Projection"; }

uint8_t Projection::num_in_tables() const { return 1; }

uint8_t Projection::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> Projection::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Projection>(_input_left->recreate(args), _column_expressions);
}

std::shared_ptr<const Table> Projection::on_execute() {
  auto output = std::make_shared<Table>();

  // Prepare terms and output table for each column to project
  for (const auto& column_expression : _column_expressions) {
    std::string name;
    if (column_expression->alias()) {
      name = *column_expression->alias();
    } else {
      name = column_expression->to_expression_string();
    }

    const auto type = evaluate_expression_type(column_expression,
      input_table_left());

    output->add_column_definition(name, type);
  }

  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    // fill the new table
    Chunk chunk_out;
    // if there is mvcc information, we have to link it
    if (input_table_left()->get_chunk(chunk_id).has_mvcc_columns()) {
      chunk_out.use_mvcc_columns_from(input_table_left()->get_chunk(chunk_id));
    }
    for (ColumnID columnID{0}; columnID < _column_expressions.size(); ++columnID) {
      call_functor_by_column_type<ColumnCreator>(output->column_type(columnID), chunk_out, chunk_id,
                                                 _column_expressions[columnID],
                                                 input_table_left());
    }
    output->add_chunk(std::move(chunk_out));
  }
  return output;
}

std::string Projection::evaluate_expression_type(
  const std::shared_ptr<ExpressionNode> & expression,
  const std::shared_ptr<const Table> & table) {

  if (expression->type() == ExpressionType::Literal) {
    return type_by_all_type_variant_which[expression->value().which()];
  }
  if (expression->type() == ExpressionType::ColumnReference) {
    return table->column_type(table->column_id_by_name(expression->name()));
  }

  Assert(expression->is_arithmetic_operator(), "Only arithmetic operators supported for expression type evaluation");

  /**
   * TODO: int + float = float etc...
   */

  const auto type_left = evaluate_expression_type(expression->left_child(), table);
  const auto type_right = evaluate_expression_type(expression->right_child(), table);

  Assert(type_left == type_right, "");
  return type_left;
}

}  // namespace opossum
