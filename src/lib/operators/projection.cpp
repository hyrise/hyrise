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

Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const ColumnExpressions& column_expressions)
    : AbstractReadOnlyOperator(in), _column_expressions(column_expressions) {}

const std::string Projection::name() const { return "Projection"; }

uint8_t Projection::num_in_tables() const { return 1; }

uint8_t Projection::num_out_tables() const { return 1; }

const Projection::ColumnExpressions& Projection::column_expressions() const { return _column_expressions; }

std::shared_ptr<AbstractOperator> Projection::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Projection>(_input_left->recreate(args), _column_expressions);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  auto output = std::make_shared<Table>();

  // Prepare terms and output table for each column to project
  for (const auto& column_expression : _column_expressions) {
    std::string name;

    if (column_expression->alias()) {
      name = *column_expression->alias();
    } else if (column_expression->type() == ExpressionType::Column) {
      name = _input_table_left()->column_name(column_expression->column_id());
    } else if (column_expression->is_arithmetic_operator() || column_expression->type() == ExpressionType::Literal) {
      name = column_expression->to_string(_input_table_left()->column_names());
    } else {
      Fail("Expression type is not supported.");
    }

    if (column_expression->is_null_literal()) {
      // in case of a NULL literal, simply add a nullable int column
      output->add_column_definition(name, "int", true);
    } else {
      const auto type = get_type_of_expression(column_expression, _input_table_left());
      output->add_column_definition(name, type);
    }
  }

  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); ++chunk_id) {
    // fill the new table
    Chunk chunk_out;

    // if there is mvcc information, we have to link it
    if (_input_table_left()->get_chunk(chunk_id).has_mvcc_columns()) {
      chunk_out.use_mvcc_columns_from(_input_table_left()->get_chunk(chunk_id));
    }

    for (uint16_t expression_index = 0u; expression_index < _column_expressions.size(); ++expression_index) {
      resolve_data_type(output->column_type(ColumnID{expression_index}), [&](auto type) {
        create_column(type, chunk_out, chunk_id, this->_column_expressions[expression_index],
                      this->_input_table_left());
      });
    }

    output->add_chunk(std::move(chunk_out));
  }

  return output;
}

const std::string Projection::get_type_of_expression(const std::shared_ptr<Expression>& expression,
                                                     const std::shared_ptr<const Table>& table) {
  if (expression->type() == ExpressionType::Literal) {
    return type_string_from_all_type_variant(expression->value());
  }
  if (expression->type() == ExpressionType::Column) {
    return table->column_type(expression->column_id());
  }

  Assert(expression->is_arithmetic_operator(),
         "Only literals, columns, and arithmetic operators supported for expression type evaluation");

  const auto type_left = get_type_of_expression(expression->left_child(), table);
  const auto type_right = get_type_of_expression(expression->right_child(), table);

  // TODO(anybody): int + float = float etc...
  // This is currently not supported by `evaluate_expression()` because it is only templated once.
  Assert(type_left == type_right, "Projection currently only supports expressions with same type on both sides.");
  return type_left;
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
