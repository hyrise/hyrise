#include "expression_evaluator.hpp"

#include "pqp_column_expression.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"
#include "storage/materialize.hpp"
#include "utils/assert.hpp"

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk):
_chunk(chunk)
{
  _column_materializations.resize(_chunk->column_count());
}

void ExpressionEvaluator::_ensure_column_materialization(const ColumnID column_id) {
  Assert(column_id < _chunk->column_count(), "Column out of range");

  if (_column_materializations[column_id]) return;

  const auto& column = *_chunk->get_column(column_id);

  resolve_data_type(column.data_type(), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    auto column_materialization = std::make_unique<ColumnMaterialization<ColumnDataType>>();
    materialize_values(column, column_materialization->values);
    _column_materializations[column_id] = std::move(column_materialization);
  });
}

std::shared_ptr<BaseColumn> ExpressionEvaluator::evaluate_expression_to_column(const AbstractExpression& expression) {
  const auto data_type = expression.data_type();

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

template<>
ExpressionEvaluator::ExpressionResult<int32_t> ExpressionEvaluator::evaluate_logical_expression<int32_t>(const LogicalExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.logical_operator) {
    case LogicalOperator::Or:  return evaluate_binary_expression<int32_t, Or>(left, right);
    case LogicalOperator::And: return evaluate_binary_expression<int32_t, And>(left, right);
  }
  // clang-format on
}

template<>
ExpressionEvaluator::ExpressionResult<int32_t> ExpressionEvaluator::evaluate_in_expression<int32_t>(const InExpression& in_expression) {
  const auto& left_expression = *in_expression.value();
  const auto& right_expression = *in_expression.set();

  std::vector<int32_t> result_values;
  std::vector<bool> result_nulls;

  if (right_expression.type == ExpressionType::Array) {
    const auto& array_expression = static_cast<const ArrayExpression&>(right_expression);

    /**
     * To keep the code simply for now, transform the InExpression like this:
     * "a IN (x, y, z)"   ---->   "a = x OR a = y OR a = z"
     *
     * But first, out of array_expression.elements(), pick those expressions whose type can be compared with
     * in_expression.value() so we're not getting "Can't compare Int and String" when doing something crazy like
     * "5 IN (6, 5, "Hello")
     */
    const auto left_is_string = left_expression.data_type() == DataType::String;
    std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
    for (const auto& element : array_expression.elements()) {
      if ((element->data_type() == DataType::String) == left_is_string) {
        type_compatible_elements.emplace_back(element);
      }
    }

    if (type_compatible_elements.empty()) {
      // NULL IN () is NULL, <not_null> IN () is FALSE
      Fail("Not supported yet");
    }

    std::shared_ptr<AbstractExpression> predicate_disjunction = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, in_expression.value(), type_compatible_elements.front());
    for (auto element_idx = size_t{1}; element_idx < type_compatible_elements.size(); ++element_idx) {
      const auto equals_element = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, in_expression.value(), type_compatible_elements[element_idx]);
      predicate_disjunction = std::make_shared<LogicalExpression>(LogicalOperator::Or, predicate_disjunction, equals_element);
    }

    return evaluate_expression<int32_t>(*predicate_disjunction);
  } else if (right_expression.type == ExpressionType::Select) {
    Fail("Unsupported ExpressionType used in InExpression");
  } else {
    Fail("Unsupported ExpressionType used in InExpression");
  }

  return {};
}

}  // namespace opossum