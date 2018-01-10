#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const ColumnExpressions& column_expressions)
    : AbstractReadOnlyOperator(in), _column_expressions(column_expressions) {}

const std::string Projection::name() const { return "Projection"; }

const Projection::ColumnExpressions& Projection::column_expressions() const { return _column_expressions; }

std::shared_ptr<AbstractOperator> Projection::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Projection>(_input_left->recreate(args), _column_expressions);
}

template <typename T>
void Projection::_create_column(boost::hana::basic_type<T> type, const std::shared_ptr<Chunk>& chunk,
                                const ChunkID chunk_id, const std::shared_ptr<Expression>& expression,
                                std::shared_ptr<const Table> input_table_left) {
  // check whether term is a just a simple column and bypass this column
  if (expression->type() == ExpressionType::Column) {
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
      non_null_values.push_back(value ? *value : T{});
      null_values.push_back(value == std::nullopt);
    }

    column = std::make_shared<ValueColumn<T>>(std::move(non_null_values), std::move(null_values));
  }

  chunk->add_column(column);
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

    // if there is mvcc information, we have to link it
    if (_input_table_left()->get_chunk(chunk_id)->has_mvcc_columns()) {
      chunk_out->use_mvcc_columns_from(_input_table_left()->get_chunk(chunk_id));
    }

    for (uint16_t expression_index = 0u; expression_index < _column_expressions.size(); ++expression_index) {
      resolve_data_type(output->column_type(ColumnID{expression_index}), [&](auto type) {
        _create_column(type, chunk_out, chunk_id, _column_expressions[expression_index], _input_table_left());
      });
    }

    output->emplace_chunk(std::move(chunk_out));
  }

  return output;
}

DataType Projection::_get_type_of_expression(const std::shared_ptr<Expression>& expression,
                                             const std::shared_ptr<const Table>& table) {
  if (expression->type() == ExpressionType::Literal) {
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
const pmr_concurrent_vector<std::optional<T>> Projection::_evaluate_expression(
    const std::shared_ptr<Expression>& expression, const std::shared_ptr<const Table> table, const ChunkID chunk_id) {
  /**
   * Handle Literal
   * This is only used if the Literal represents a constant column, e.g. in 'SELECT 5 FROM table_a'.
   * On the other hand this is not used for nested arithmetic Expressions, such as 'SELECT a + 5 FROM table_a'.
   */
  if (expression->type() == ExpressionType::Literal) {
    return pmr_concurrent_vector<std::optional<T>>(table->get_chunk(chunk_id)->size(),
                                                   boost::get<T>(expression->value()));
  }

  /**
   * Handle column reference
   */
  if (expression->type() == ExpressionType::Column) {
    auto column = table->get_chunk(chunk_id)->get_column(expression->column_id());

    if (auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column)) {
      // values are copied
      return value_column->materialize_values();
    }
    if (auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column)) {
      return dict_column->materialize_values();
    }
    if (auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column)) {
      return ref_column->template materialize_values<T>();  // Clang needs the template prefix
    }

    Fail("Materializing chunk failed.");
  }

  /**
   * Handle arithmetic expression
   */
  Assert(expression->is_arithmetic_operator(), "Projection only supports literals, column refs and arithmetics");

  const auto& arithmetic_operator_function = _get_operator_function<T>(expression->type());

  pmr_concurrent_vector<std::optional<T>> values;
  values.resize(table->get_chunk(chunk_id)->size());

  const auto& left = expression->left_child();
  const auto& right = expression->right_child();
  const auto left_is_literal = left->type() == ExpressionType::Literal;
  const auto right_is_literal = right->type() == ExpressionType::Literal;

  if ((left_is_literal && variant_is_null(left->value())) || (right_is_literal && variant_is_null(right->value()))) {
    // one of the operands is a literal null - early out.
    std::fill(values.begin(), values.end(), std::nullopt);
  } else if (left_is_literal && right_is_literal) {
    std::fill(values.begin(), values.end(),
              arithmetic_operator_function(boost::get<T>(left->value()), boost::get<T>(right->value())));
  } else if (right_is_literal) {
    auto left_values = _evaluate_expression<T>(left, table, chunk_id);
    auto right_value = boost::get<T>(right->value());
    // apply operator function to both vectors
    auto func = [&](std::optional<T> left_value) -> std::optional<T> {
      if (!left_value) return std::nullopt;
      return arithmetic_operator_function(*left_value, right_value);
    };
    std::transform(left_values.begin(), left_values.end(), values.begin(), func);

  } else if (left_is_literal) {
    auto right_values = _evaluate_expression<T>(right, table, chunk_id);
    auto left_value = boost::get<T>(left->value());
    // apply operator function to both vectors
    auto func = [&](std::optional<T> right_value) -> std::optional<T> {
      if (!right_value) return std::nullopt;
      return arithmetic_operator_function(left_value, *right_value);
    };
    std::transform(right_values.begin(), right_values.end(), values.begin(), func);

  } else {
    auto left_values = _evaluate_expression<T>(left, table, chunk_id);
    auto right_values = _evaluate_expression<T>(right, table, chunk_id);

    // apply operator function to both vectors
    auto func = [&](std::optional<T> left, std::optional<T> right) -> std::optional<T> {
      if (!left || !right) return std::nullopt;
      return arithmetic_operator_function(*left, *right);
    };
    std::transform(left_values.begin(), left_values.end(), right_values.begin(), values.begin(), func);
  }

  return values;
}

template <typename T>
std::function<T(const T&, const T&)> Projection::_get_base_operator_function(ExpressionType type) {
  switch (type) {
    case ExpressionType::Addition:
      return std::plus<T>();
    case ExpressionType::Subtraction:
      return std::minus<T>();
    case ExpressionType::Multiplication:
      return std::multiplies<T>();
    case ExpressionType::Division:
      return std::divides<T>();

    default:
      Fail("Unknown arithmetic operator");
      return {};
  }
}

template <typename T>
std::function<T(const T&, const T&)> Projection::_get_operator_function(ExpressionType type) {
  if (type == ExpressionType::Modulo) return std::modulus<T>();
  return _get_base_operator_function<T>(type);
}

/**
 * Specialized arithmetic operator implementation for std::string.
 * Two string terms can be added. Anything else is undefined.
 *
 * @returns a lambda function to solve arithmetic string terms
 *
 */
template <>
inline std::function<std::string(const std::string&, const std::string&)> Projection::_get_operator_function(
    ExpressionType type) {
  Assert(type == ExpressionType::Addition, "Arithmetic operator except for addition not defined for std::string");
  return std::plus<std::string>();
}

/**
 * Specialized arithmetic operator implementation for float/double
 * Modulo on float isn't defined.
 *
 * @returns a lambda function to solve arithmetic float/double terms
 *
 */
template <>
inline std::function<float(const float&, const float&)> Projection::_get_operator_function(ExpressionType type) {
  return _get_base_operator_function<float>(type);
}

template <>
inline std::function<double(const double&, const double&)> Projection::_get_operator_function(ExpressionType type) {
  return _get_base_operator_function<double>(type);
}

/**
 * Specialized arithmetic operator implementation for int
 * Division by 0 needs to be caught when using integers.
 */
template <>
inline std::function<int(const int&, const int&)> Projection::_get_operator_function(ExpressionType type) {
  if (type == ExpressionType::Division) {
    return [](const int& lhs, const int& rhs) {
      if (rhs == 0) {
        throw std::runtime_error("Cannot divide integers by 0.");
      }
      return lhs / rhs;
    };
  }
  return _get_base_operator_function<int>(type);
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
