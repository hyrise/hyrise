#include "expression_evaluator.hpp"

#include "pqp_column_expression.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk):
_chunk(chunk)
{}

std::shared_ptr<BaseColumn> ExpressionEvaluator::evaluate_expression_to_column(const AbstractExpression& expression) {
  const auto data_type = boost::get<DataType>(expression.data_type());

  std::shared_ptr<BaseColumn> column;

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto result = evaluate_expression<ColumnDataType>(expression);

    pmr_concurrent_vector<ColumnDataType> values;
    pmr_concurrent_vector<bool> nulls;

    auto has_nulls = false;

    if (result.type() == typeid(ExpressionEvaluator::NonNullableValues<ColumnDataType>)) {
      const auto& result_values = boost::get<ExpressionEvaluator::NonNullableValues<ColumnDataType>>(result);
      values = pmr_concurrent_vector<ColumnDataType>(result_values.begin(), result_values.end());

    } else if (result.type() == typeid(ExpressionEvaluator::NullableValues<ColumnDataType>)) {
      const auto& result_values_and_nulls = boost::get<ExpressionEvaluator::NullableValues<ColumnDataType>>(result);
      const auto& result_values = result_values_and_nulls.first;
      const auto& result_nulls = result_values_and_nulls.second;
      has_nulls = true;

      values = pmr_concurrent_vector<ColumnDataType>(result_values.begin(), result_values.end());
      nulls = pmr_concurrent_vector<bool>(result_nulls.begin(), result_nulls.end());

    } else if (result.type() == typeid(NullValue)) {
      values.resize(_chunk->size(), ColumnDataType{});
      nulls.resize(_chunk->size(), true);
      has_nulls = true;

    } else if (result.type() == typeid(ColumnDataType)) {
      values.resize(_chunk->size(), boost::get<ColumnDataType>(result));

    }

    if (has_nulls) {
      column = std::make_shared<ValueColumn<ColumnDataType>>(std::move(values), std::move(nulls));
    } else {
      column = std::make_shared<ValueColumn<ColumnDataType>>(std::move(values));
    }
  });

  return column;
}

}  // namespace opossum