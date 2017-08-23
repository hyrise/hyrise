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

    const auto type = get_type_of_expression(column_expression, input_table_left());

    output->add_column_definition(name, type);
  }

  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    // fill the new table
    Chunk chunk_out;

    // if there is mvcc information, we have to link it
    if (input_table_left()->get_chunk(chunk_id).has_mvcc_columns()) {
      chunk_out.use_mvcc_columns_from(input_table_left()->get_chunk(chunk_id));
    }

    for (uint16_t expression_index = 0u; expression_index < _column_expressions.size(); ++expression_index) {
      call_functor_by_column_type<ColumnCreator>(output->column_type(ColumnID{expression_index}), chunk_out, chunk_id,
                                                 _column_expressions[expression_index], input_table_left());
    }

    output->add_chunk(std::move(chunk_out));
  }
  return output;
}

const std::string Projection::get_type_of_expression(const std::shared_ptr<ExpressionNode>& expression,
                                                     const std::shared_ptr<const Table>& table) {
  if (expression->type() == ExpressionType::Literal) {
    return type_string_from_all_type_variant(expression->value());
  }
  if (expression->type() == ExpressionType::ColumnIdentifier) {
    return table->column_type(table->column_id_by_name(expression->name()));
  }

  Assert(expression->is_arithmetic_operator(),
    "Only literals, columns, and arithmetic operators supported for expression type evaluation");

  const auto type_left = get_type_of_expression(expression->left_child(), table);
  const auto type_right = get_type_of_expression(expression->right_child(), table);

  /**
   * Type promotion - int + long is long, etc.
   * This does a lot of string comparing... :( :(
   * TODO(anyone): Convert types into enums
   */
  auto result_type = type_left;
  auto type_promotion = [&] (const auto & a, const auto & b, const auto & result) mutable {
    if ((type_left == a && type_right == b) ||
        (type_left == b && type_right == a)) {
      result_type = result;
    }
  };

  type_promotion("int", "long", "long");
  type_promotion("int", "float", "float");
  type_promotion("int", "double", "double");
  type_promotion("long", "float", "double");
  type_promotion("long", "double", "double");
  type_promotion("float", "double", "double");

  return result_type;
}

}  // namespace opossum
