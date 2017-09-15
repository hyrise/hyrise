#pragma once

#include <cstdint>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "optimizer/expression.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Operator to select a subset of the set of all columns found in the table
 *
 * Note: Projection does not support null values at the moment
 */
class Projection : public AbstractReadOnlyOperator {
 public:
  using ColumnExpressions = std::vector<std::shared_ptr<Expression>>;

  Projection(const std::shared_ptr<const AbstractOperator> in, const ColumnExpressions& column_expressions);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  const ColumnExpressions& column_expressions() const;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  /**
   * The dummy table is used for literal projections that have no input table.
   * This was introduce to allow queries like INSERT INTO tbl VALUES (1, 2, 3);
   * Because each INSERT uses a projection as input, the above case needs to project the three
   * literals (1, 2, 3) without any specific input table. Therefore, this dummy table is used instead.
   *
   * The dummy table contains one (value) column with one row. This way, the above projection
   * contains exactly one row with the given literals.
   */
  class DummyTable : public Table {
   public:
    DummyTable() : Table(0) {
      add_column("dummy", "int");
      append(std::vector<AllTypeVariant>{0});
    }
  };

  static std::shared_ptr<Table> dummy_table();

 protected:
  ColumnExpressions _column_expressions;

  template <typename T>
  static void create_column(boost::hana::basic_type<T> type, Chunk& chunk, const ChunkID chunk_id,
                            const std::shared_ptr<Expression>& expression,
                            std::shared_ptr<const Table> input_table_left) {
    // check whether term is a just a simple column and bypass this column
    if (expression->type() == ExpressionType::Column) {
      auto bypassed_column = input_table_left->get_chunk(chunk_id).get_column(expression->column_id());
      return chunk.add_column(bypassed_column);
    }

    std::shared_ptr<BaseColumn> column;

    if (expression->is_null_literal()) {
      // fill a nullable column with NULLs
      auto row_count = input_table_left->get_chunk(chunk_id).size();
      auto null_values = pmr_concurrent_vector<bool>(row_count, true);
      auto values = pmr_concurrent_vector<T>(row_count);

      column = std::make_shared<ValueColumn<T>>(std::move(values), std::move(null_values));
    } else {
      // fill a value column with the specified literal
      auto values = evaluate_expression<T>(expression, input_table_left, chunk_id);
      column = std::make_shared<ValueColumn<T>>(std::move(values));
    }

    chunk.add_column(column);
  }

  static const std::string get_type_of_expression(const std::shared_ptr<Expression>& expression,
                                                  const std::shared_ptr<const Table>& table);

  /**
   * This function evaluates the given expression on a single chunk.
   * It returns a vector containing the materialized values resulting from the expression.
   */
  template <typename T>
  static const tbb::concurrent_vector<T> evaluate_expression(const std::shared_ptr<Expression>& expression,
                                                             const std::shared_ptr<const Table> table,
                                                             const ChunkID chunk_id) {
    /**
     * Handle Literal
     * This is only used if the Literal represents a constant column, e.g. in 'SELECT 5 FROM table_a'.
     * On the other hand this is not used for nested arithmetic Expressions, such as 'SELECT a + 5 FROM table_a'.
     */
    if (expression->type() == ExpressionType::Literal) {
      return tbb::concurrent_vector<T>(table->get_chunk(chunk_id).size(), boost::get<T>(expression->value()));
    }

    /**
     * Handle column reference
     */
    if (expression->type() == ExpressionType::Column) {
      auto column = table->get_chunk(chunk_id).get_column(expression->column_id());

      if (auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
        // values are copied
        return value_column->values();
      }
      if (auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
        return dict_column->materialize_values();
      }
      if (auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
        return ref_column->template materialize_values<T>();  // Clang needs the template prefix
      }

      Fail("Materializing chunk failed.");
    }

    /**
     * Handle arithmetic expression
     */
    Assert(expression->is_arithmetic_operator(), "Projection only supports literals, column refs and arithmetics");

    const auto& arithmetic_operator_function = get_operator_function<T>(expression->type());

    tbb::concurrent_vector<T> values;
    values.resize(table->get_chunk(chunk_id).size());

    const auto& left = expression->left_child();
    const auto& right = expression->right_child();
    const auto left_is_literal = left->type() == ExpressionType::Literal;
    const auto right_is_literal = right->type() == ExpressionType::Literal;

    if (left_is_literal && right_is_literal) {
      std::fill(values.begin(), values.end(),
                arithmetic_operator_function(boost::get<T>(left->value()), boost::get<T>(right->value())));
    } else if (right_is_literal) {
      auto left_values = evaluate_expression<T>(left, table, chunk_id);
      auto right_value = boost::get<T>(right->value());
      // apply operator function to both vectors
      std::transform(left_values.begin(), left_values.end(), values.begin(),
                     [&](T left_value) { return arithmetic_operator_function(left_value, right_value); });

    } else if (left_is_literal) {
      auto right_values = evaluate_expression<T>(right, table, chunk_id);
      auto left_value = boost::get<T>(left->value());
      // apply operator function to both vectors
      std::transform(right_values.begin(), right_values.end(), values.begin(),
                     [&](T right_value) { return arithmetic_operator_function(left_value, right_value); });

    } else {
      auto left_values = evaluate_expression<T>(left, table, chunk_id);
      auto right_values = evaluate_expression<T>(right, table, chunk_id);

      // apply operator function to both vectors
      std::transform(left_values.begin(), left_values.end(), right_values.begin(), values.begin(),
                     arithmetic_operator_function);
    }

    return values;
  }

  /**
   * Operators that all numerical types support.
   */
  template <typename T>
  static std::function<T(const T&, const T&)> get_base_operator_function(ExpressionType type) {
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

  /**
   * Operators that integral types support.
   */
  template <typename T>
  static std::function<T(const T&, const T&)> get_operator_function(ExpressionType type) {
    if (type == ExpressionType::Modulo) return std::modulus<T>();
    return get_base_operator_function<T>(type);
  }

  std::shared_ptr<const Table> _on_execute() override;
};

/**
 * Specialized arithmetic operator implementation for std::string.
 * Two string terms can be added. Anything else is undefined.
 *
 * @returns a lambda function to solve arithmetic string terms
 *
 */
template <>
inline std::function<std::string(const std::string&, const std::string&)> Projection::get_operator_function(
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
inline std::function<float(const float&, const float&)> Projection::get_operator_function(ExpressionType type) {
  return get_base_operator_function<float>(type);
}

template <>
inline std::function<double(const double&, const double&)> Projection::get_operator_function(ExpressionType type) {
  return get_base_operator_function<double>(type);
}

}  // namespace opossum
