#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/pqp_expression.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/materialize.hpp"
#include "storage/reference_column.hpp"
#include "utils/arithmetic_operator_expression.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const ColumnExpressions& column_expressions)
    : AbstractReadOnlyOperator(in), _column_expressions(column_expressions) {}

const std::string Projection::name() const { return "Projection"; }

const std::string Projection::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  desc << "[Projection] ";
  for (size_t expression_idx = 0; expression_idx < _column_expressions.size(); ++expression_idx) {
    desc << _column_expressions[expression_idx]->description();
    if (expression_idx + 1 < _column_expressions.size()) {
      desc << ", ";
    }
  }
  return desc.str();
}

const Projection::ColumnExpressions& Projection::column_expressions() const { return _column_expressions; }

std::shared_ptr<AbstractOperator> Projection::recreate(const std::vector<AllParameterVariant>& args) const {
  ColumnExpressions new_column_expressions;

  for (const auto& column_expression : _column_expressions) {
    if (column_expression->type() == ExpressionType::Placeholder) {
      auto value_placeholder = column_expression->value_placeholder();

      if (value_placeholder.index() < args.size()) {
        const auto& parameter_variant = args[value_placeholder.index()];
        auto value = boost::get<AllTypeVariant>(parameter_variant);
        new_column_expressions.emplace_back(column_expression->set_placeholder_value(value));
      }
    } else {
      new_column_expressions.emplace_back(column_expression);
    }
  }

  return std::make_shared<Projection>(_input_left->recreate(args), new_column_expressions);
}

template <typename T>
void Projection::_create_column(boost::hana::basic_type<T> type, const std::shared_ptr<Chunk>& chunk,
                                const ChunkID chunk_id, const std::shared_ptr<PQPExpression>& expression,
                                std::shared_ptr<const Table> input_table_left, bool reuse_column_from_input) {
  // check whether term is a just a simple column and bypass this column
  if (reuse_column_from_input) {
    // we have to use get_mutable_column here because we cannot add a const column to the chunk
    auto bypassed_column = input_table_left->get_chunk(chunk_id)->get_mutable_column(expression->column_id());
    return chunk->add_column(bypassed_column);
  }

  std::shared_ptr<BaseColumn> column;

  if (expression->is_null_literal()) {
    // fill a nullable column with NULLs
    auto row_count = input_table_left->get_chunk(chunk_id)->size();
    auto null_values = pmr_concurrent_vector<bool>(row_count, true);
    // Explicitly pass T{} because in some cases it won't initialize otherwise
    auto values = pmr_concurrent_vector<T>(row_count, T{});

    column = std::make_shared<ValueColumn<T>>(std::move(values), std::move(null_values));
  } else {
    // fill a value column with the specified expression
    auto values = _evaluate_expression<T>(expression, input_table_left, chunk_id);

    pmr_concurrent_vector<T> non_null_values;
    non_null_values.reserve(values.size());
    pmr_concurrent_vector<bool> null_values;
    null_values.reserve(values.size());

    for (const auto value : values) {
      non_null_values.push_back(value.second);
      null_values.push_back(value.first);
    }

    column = std::make_shared<ValueColumn<T>>(std::move(non_null_values), std::move(null_values));
  }

  chunk->add_column(column);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  auto output = std::make_shared<Table>();
  auto reuse_column_from_input = true;

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

    if (column_expression->type() != ExpressionType::Column) {
      reuse_column_from_input = false;
    }

    const auto type = _get_type_of_expression(column_expression, _input_table_left());
    if (type == DataType::Null) {
      // in case of a NULL literal, simply add a nullable int column
      output->add_column_definition(name, DataType::Int, true);
    } else {
      output->add_column_definition(name, type);
    }
  }

  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); ++chunk_id) {
    // fill the new table
    auto chunk_out = std::make_shared<Chunk>();

    for (uint16_t expression_index = 0u; expression_index < _column_expressions.size(); ++expression_index) {
      resolve_data_type(output->column_type(ColumnID{expression_index}), [&](auto type) {
        _create_column(type, chunk_out, chunk_id, _column_expressions[expression_index], _input_table_left(),
                       reuse_column_from_input);
      });
    }

    output->emplace_chunk(std::move(chunk_out));
  }

  return output;
}

DataType Projection::_get_type_of_expression(const std::shared_ptr<PQPExpression>& expression,
                                             const std::shared_ptr<const Table>& table) {
  if (expression->type() == ExpressionType::Literal || expression->type() == ExpressionType::Placeholder) {
    return data_type_from_all_type_variant(expression->value());
  }
  if (expression->type() == ExpressionType::Column) {
    return table->column_type(expression->column_id());
  }

  Assert(expression->is_arithmetic_operator(),
         "Only literals, columns, and arithmetic operators supported for expression type evaluation");

  const auto type_left = _get_type_of_expression(expression->left_child(), table);
  const auto type_right = _get_type_of_expression(expression->right_child(), table);

  if (type_left == DataType::Null) return type_right;
  if (type_right == DataType::Null) return type_left;

  const auto type_string_left = data_type_to_string.left.at(type_left);
  const auto type_string_right = data_type_to_string.left.at(type_right);

  // TODO(anybody): int + float = float etc...
  // This is currently not supported by `_evaluate_expression()` because it is only templated once.
  Assert(type_left == type_right, "Projection currently only supports expressions with same type on both sides (" +
                                      type_string_left + " vs " + type_string_right + ")");
  return type_left;
}

template <typename T>
const pmr_concurrent_vector<std::pair<bool, T>> Projection::_evaluate_expression(
    const std::shared_ptr<PQPExpression>& expression, const std::shared_ptr<const Table> table,
    const ChunkID chunk_id) {
  /**
   * Handle Literal
   * This is only used if the Literal represents a constant column, e.g. in 'SELECT 5 FROM table_a'.
   * On the other hand this is not used for nested arithmetic Expressions, such as 'SELECT a + 5 FROM table_a'.
   */
  if (expression->type() == ExpressionType::Literal) {
    return pmr_concurrent_vector<std::pair<bool, T>>(table->get_chunk(chunk_id)->size(),
                                                     std::make_pair(false, boost::get<T>(expression->value())));
  }

  /**
   * Handle column reference
   */
  if (expression->type() == ExpressionType::Column) {
    const auto chunk = table->get_chunk(chunk_id);

    pmr_concurrent_vector<std::pair<bool, T>> values_and_nulls;
    values_and_nulls.reserve(chunk->size());

    materialize_values_and_nulls(*chunk->get_column(expression->column_id()), values_and_nulls);

    return values_and_nulls;
  }

  /**
   * Handle arithmetic expression
   */
  Assert(expression->is_arithmetic_operator(), "Projection only supports literals, column refs and arithmetics");

  const auto& arithmetic_operator_function = function_for_arithmetic_expression<T>(expression->type());

  pmr_concurrent_vector<std::pair<bool, T>> values;
  values.resize(table->get_chunk(chunk_id)->size());

  const auto& left = expression->left_child();
  const auto& right = expression->right_child();
  const auto left_is_literal = left->type() == ExpressionType::Literal;
  const auto right_is_literal = right->type() == ExpressionType::Literal;

  if ((left_is_literal && variant_is_null(left->value())) || (right_is_literal && variant_is_null(right->value()))) {
    // one of the operands is a literal null - early out.
    std::fill(values.begin(), values.end(), std::make_pair(true, T{}));
  } else if (left_is_literal && right_is_literal) {
    std::fill(values.begin(), values.end(),
              std::make_pair(
                  false, arithmetic_operator_function(boost::get<T>(left->value()), boost::get<T>(right->value()))));
  } else if (right_is_literal) {
    auto left_values = _evaluate_expression<T>(left, table, chunk_id);
    auto right_value = boost::get<T>(right->value());
    // apply operator function to both vectors
    auto func = [&](const std::pair<bool, T>& left_value) -> std::pair<bool, T> {
      return std::make_pair(left_value.first, arithmetic_operator_function(left_value.second, right_value));
    };
    std::transform(left_values.begin(), left_values.end(), values.begin(), func);
  } else if (left_is_literal) {
    auto right_values = _evaluate_expression<T>(right, table, chunk_id);
    auto left_value = boost::get<T>(left->value());
    // apply operator function to both vectors
    auto func = [&](const std::pair<bool, T>& right_value) -> std::pair<bool, T> {
      return std::make_pair(right_value.first, arithmetic_operator_function(left_value, right_value.second));
    };
    std::transform(right_values.begin(), right_values.end(), values.begin(), func);
  } else {
    auto left_values = _evaluate_expression<T>(left, table, chunk_id);
    auto right_values = _evaluate_expression<T>(right, table, chunk_id);

    // apply operator function to both vectors
    auto func = [&](const std::pair<bool, T>& left, const std::pair<bool, T>& right) -> std::pair<bool, T> {
      return std::make_pair(left.first || right.first, arithmetic_operator_function(left.second, right.second));
    };
    std::transform(left_values.begin(), left_values.end(), right_values.begin(), values.begin(), func);
  }

  return values;
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
