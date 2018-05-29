#include "expression_evaluator.hpp"

#include <iterator>
#include <type_traits>

#include "boost/variant/apply_visitor.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/expression_factory.hpp"
#include "all_parameter_variant.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/materialize.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"
#include "resolve_type.hpp"
#include "utils/lambda_visitor.hpp"
#include "storage/value_column.hpp"
#include "utils/assert.hpp"
#include "expression_functors.hpp"

using namespace std::string_literals;
using namespace opossum::expression_factory;

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk):
_chunk(chunk)
{
  _output_row_count = _chunk->size();
  _column_materializations.resize(_chunk->column_count());
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_expression(const AbstractExpression& expression) {
  switch (expression.type) {
    case ExpressionType::Arithmetic:
      return evaluate_arithmetic_expression<T>(static_cast<const ArithmeticExpression&>(expression));

    case ExpressionType::Logical:
      return evaluate_logical_expression<T>(static_cast<const LogicalExpression&>(expression));

    case ExpressionType::Predicate: {
      const auto& predicate_expression = static_cast<const AbstractPredicateExpression&>(expression);

      if (is_lexicographical_predicate_condition(predicate_expression.predicate_condition)) {
        return evaluate_binary_predicate_expression<T>(static_cast<const BinaryPredicateExpression&>(expression));
      } else if (predicate_expression.predicate_condition == PredicateCondition::In) {
        Fail("Unsupported Predicate Expression");
        //return evaluate_in_expression<T>(static_cast<const InExpression&>(expression));
      } else {
        Fail("Unsupported Predicate Expression");
      }
    }

//    case ExpressionType::Select: {
//      const auto* pqp_select_expression = dynamic_cast<const PQPSelectExpression*>(&expression);
//      Assert(pqp_select_expression, "Can only evaluate PQPSelectExpression");

//      return evaluate_select_expression_for_chunk<T>(*pqp_select_expression);
//    }

    case ExpressionType::Column: {
      Assert(_chunk, "Cannot access Columns in this Expression as it doesn't operate on a Table/Chunk");

      const auto *pqp_column_expression = dynamic_cast<const PQPColumnExpression *>(&expression);
      Assert(pqp_column_expression, "Can only evaluate PQPColumnExpressions");

      const auto &column = *_chunk->get_column(pqp_column_expression->column_id);

      std::vector<T> values;
      materialize_values(column, values);

      if (pqp_column_expression->is_nullable()) {
        std::vector<bool> nulls;
        materialize_nulls<T>(column, nulls);

        return ExpressionResult<T>(std::move(values), std::move(nulls));

      } else {
        return ExpressionResult<T>{std::move(values)};

      }
    }

    case ExpressionType::Value: {
      const auto& value_expression = static_cast<const ValueExpression &>(expression);
      const auto& value = value_expression.value;

      if (value.type() == typeid(NullValue)) {
        // NullValue can be evaluated to any type - it is then a null value of that type.
        // This makes it easier to implement expressions where a certain data type is expected, but a Null literal is
        // given. Think `CASE NULL THEN ... ELSE ...` - the NULL will be evaluated to be a bool.
        return ExpressionResult<T>{{{T{}}}, {true}};
      } else {
        Assert(value.type() == typeid(T), "Can't evaluate ValueExpression to requested type T");
        return ExpressionResult<T>{{{boost::get<T>(value)}}};
      }
    }
//
//    case ExpressionType::Function:
//      return evaluate_function_expression<T>(static_cast<const FunctionExpression&>(expression));
//
    case ExpressionType::Case:
      return evaluate_case_expression<T>(static_cast<const CaseExpression&>(expression));
//
//    case ExpressionType::Exists:
//      return evaluate_exists_expression<T>(static_cast<const ExistsExpression&>(expression));
//
//    case ExpressionType::Extract:
//      return evaluate_extract_expression<T>(static_cast<const ExtractExpression&>(expression));
//
//    case ExpressionType::Not:
//      Fail("Not not yet implemented");

//    case ExpressionType::Array:
//      return evaluate_array<T>(static_cast<const ArrayExpression&>(expression));

//    case ExpressionType::External:
//    case ExpressionType::ValuePlaceholder:
//    case ExpressionType::Mock:
//      Fail("Can't handle External/ValuePlaceholders/Mocks since they don't have a value.");
//
//    case ExpressionType::Aggregate:
//      Fail("ExpressionEvaluator doesn't support Aggregates, use the Aggregate Operator to compute them");

    default:
      Fail("tmpfail");
  }
}

//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_function_expression(const FunctionExpression& expression) {
//  ExpressionResult<T> result;
//
//  switch (expression.function_type) {
//    case FunctionType::Substring:
//      // clang-format off
//      if constexpr (std::is_same_v<T, std::string>) {
//        resolve_data_type(expression.arguments[1]->data_type(), [&](const auto offset_data_type_t) {
//          using OffsetDataType = typename decltype(offset_data_type_t)::type;
//
//          resolve_data_type(expression.arguments[2]->data_type(), [&](const auto char_count_data_type_t) {
//            using CharCountDataType = typename decltype(char_count_data_type_t)::type;
//
//            if constexpr(std::is_integral_v<OffsetDataType> && std::is_integral_v<CharCountDataType>) {
//              result = evaluate_substring(
//                evaluate_expression<std::string>(*expression.arguments[0]),
//                evaluate_expression<OffsetDataType>(*expression.arguments[1]),
//                evaluate_expression<OffsetDataType>(*expression.arguments[2]));
//            } else {
//              Fail("SUBSTRING parameters 2 and 3 need to be integral");
//            }
//          });
//        });
//      } else {
//        Fail("SUBSTRING can only return String");
//      }
//      // clang-format on
//  }
//
//  return result;
//}

//template<typename OffsetDataType, typename CharCountDataType>
//ExpressionResult<std::string> ExpressionEvaluator::evaluate_substring(const ExpressionResult<std::string>& string_result,
//                                                 const ExpressionResult<OffsetDataType>& offset_result,
//                                                 const ExpressionResult<CharCountDataType>& char_count_result) {
//  Fail("Not yet implemented");
////  /**
////   *
////   */
////
////  auto result_values = std::vector<std::string>(_output_row_count);
////  auto result_nulls = std::vector<bool>(_output_row_count);
////
////  for (auto chunk_offset = 0; chunk_offset < _output_row_count; ++chunk_offset) {
////
////  }
//}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_arithmetic_expression(const ArithmeticExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:       return evaluate_binary_with_default_null_logic<T, Addition>(left, right);
    case ArithmeticOperator::Subtraction:    return evaluate_binary_with_default_null_logic<T, Subtraction>(left, right);
    case ArithmeticOperator::Multiplication: return evaluate_binary_with_default_null_logic<T, Multiplication>(left, right);
    case ArithmeticOperator::Division:       return evaluate_binary_with_default_null_logic<T, Division>(left, right);

    default:
      Fail("ArithmeticOperator evaluation not yet implemented");
  }
  // clang-format on
}

template<>
ExpressionResult<int32_t> ExpressionEvaluator::evaluate_binary_predicate_expression<int32_t>(const BinaryPredicateExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.predicate_condition) {
    case PredicateCondition::Equals:            return evaluate_binary_with_default_null_logic<int32_t, Equals>(left, right);
    case PredicateCondition::NotEquals:         return evaluate_binary_with_default_null_logic<int32_t, NotEquals>(left, right);  // NOLINT
    case PredicateCondition::LessThan:          return evaluate_binary_with_default_null_logic<int32_t, LessThan>(left, right);  // NOLINT
    case PredicateCondition::LessThanEquals:    return evaluate_binary_with_default_null_logic<int32_t, LessThanEquals>(left, right);  // NOLINT
    case PredicateCondition::GreaterThan:       return evaluate_binary_with_default_null_logic<int32_t, GreaterThan>(left, right);  // NOLINT
    case PredicateCondition::GreaterThanEquals: return evaluate_binary_with_default_null_logic<int32_t, GreaterThanEquals>(left, right);  // NOLINT

    default:
      Fail("PredicateCondition evaluation not yet implemented");
  }
  // clang-format on
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression) {
  Fail("Can only evaluate binary predicate to int32_t");
}


//template<typename T, template<typename...> typename Functor>
//ExpressionResult<T> ExpressionEvaluator::evaluate_binary_expression(
//const AbstractExpression& left_operand,
//const AbstractExpression& right_operand) {
//  const auto left_is_null = left_operand.data_type() == DataType::Null;
//  const auto right_is_null = right_operand.data_type() == DataType::Null;
//
//  if (left_is_null && right_is_null) return NullValue{};
//
//  ExpressionResult<T> result;
//
//  if (left_is_null) {
//    resolve_data_type(right_operand.data_type(), [&](const auto right_data_type_t) {
//      using RightDataType = typename decltype(right_data_type_t)::type;
//      const auto right_result = evaluate_expression<RightDataType>(right_operand);
//      using ConcreteFunctor = Functor<T, T, RightDataType>;
//
//      if constexpr (ConcreteFunctor::supported) {
//        result = evaluate_binary_operator<T, T, RightDataType>(ExpressionResult<T>(NullValue{}), right_result, ConcreteFunctor{});
//      } else {
//        Fail("Operation not supported on the given types");
//      }
//    });
//  } else if (right_is_null) {
//    resolve_data_type(left_operand.data_type(), [&](const auto left_data_type_t) {
//      using LeftDataType = typename decltype(left_data_type_t)::type;
//      const auto left_result = evaluate_expression<LeftDataType>(left_operand);
//      using ConcreteFunctor = Functor<T, LeftDataType, T>;
//
//      if constexpr (ConcreteFunctor::supported) {
//        result = evaluate_binary_operator<T, LeftDataType, T>(left_result, ExpressionResult<T>(NullValue{}), ConcreteFunctor{});
//      } else {
//        Fail("Operation not supported on the given types");
//      }
//    });
//
//  } else {
//    resolve_data_type(left_operand.data_type(), [&](const auto left_data_type_t) {
//      using LeftDataType = typename decltype(left_data_type_t)::type;
//
//      const auto left_operands = evaluate_expression<LeftDataType>(left_operand);
//
//      resolve_data_type(right_operand.data_type(), [&](const auto right_data_type_t) {
//        using RightDataType = typename decltype(right_data_type_t)::type;
//
//        const auto right_operands = evaluate_expression<RightDataType>(right_operand);
//
//        using ConcreteFunctor = Functor<T, LeftDataType, RightDataType>;
//
//        if constexpr (ConcreteFunctor::supported) {
//          result = evaluate_binary_operator<T, LeftDataType, RightDataType>(left_operands, right_operands, ConcreteFunctor{});
//        } else {
//          Fail("Operation not supported on the given types");
//        }
//      });
//    });
//  }
//
//  return result;
//}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_logical_expression(const LogicalExpression& expression) {
  Fail("LogicalExpression can only output int32_t");
}
//
//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_in_expression(const InExpression& expression) {
//  Fail("InExpression supports only int32_t as result");
//}

template<typename R>
ExpressionResult<R> ExpressionEvaluator::evaluate_case_expression(const CaseExpression& case_expression) {
  const auto when = evaluate_expression<int32_t>(*case_expression.when());

  /**
   * Optimization - but block below relies on the case where WHEN is a literal to be handled separately
   *    Handle cases where the CASE condition ("WHEN") is a fixed value/NULL (e.g. CASE 5+3 > 2 THEN ... ELSE ...)
   *    This avoids computing branches we don't need to compute.
   */
  if (when.is_literal()) {
    const auto when_literal = when.to_literal();

    if (when_literal.value() && !when_literal.null()) {
      return evaluate_expression<R>(*case_expression.then());
    } else {
      return evaluate_expression<R>(*case_expression.else_());
    }
  }

  /**
   * Handle cases where the CASE condition is a series and thus we need to evaluate row by row.
   * Think `CASE a > b THEN ... ELSE ...`
   */
  std::vector<R> values(when.size());
  std::vector<bool> nulls(when.size());

  resolve_expression_result_to_view(when, [&] (const auto& when_view) {
    resolve_to_expression_result_views(*case_expression.then(), *case_expression.else_(), [&] (const auto& then_view, const auto& else_view) {
      using ThenResultType = typename std::decay_t<decltype(then_view)>::Type;
      using ElseResultType = typename std::decay_t<decltype(else_view)>::Type;

      // clang-format off
      if constexpr (Case::template supports<R, ThenResultType, ElseResultType>::value) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < when.size(); ++chunk_offset) {
          if (when_view.value(chunk_offset) && !when_view.null(chunk_offset)) {
            values[chunk_offset] = to_value<R>(then_view.value(chunk_offset));
            nulls[chunk_offset] = then_view.null(chunk_offset);
          } else {
            values[chunk_offset] = to_value<R>(else_view.value(chunk_offset));
            nulls[chunk_offset] = else_view.null(chunk_offset);
          }
        }
      }
      // clang-format on
    });
  });

  return {std::move(values), std::move(nulls)};
}
//
//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_exists_expression(const ExistsExpression& exists_expression) {
//  std::vector<T> result_values(_output_row_count);
//
//  const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(exists_expression.select());
//  for (const auto& parameter : pqp_select_expression->parameters) {
//    _ensure_column_materialization(parameter);
//  }
//
//  resolve_data_type(pqp_select_expression->data_type(), [&](const auto select_data_type_t) {
//    using SelectDataType = typename decltype(select_data_type_t)::type;
//
//    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
//      const auto select_result = evaluate_select_expression_for_row<SelectDataType>(*pqp_select_expression, chunk_offset);
//      const auto& select_result_values = boost::get<NonNullableValues<SelectDataType>>(select_result);
//      result_values[chunk_offset] = !select_result_values.empty();
//    }
//  });
//
//  return result_values;
//}
//
//template<>
//ExpressionResult<std::string> ExpressionEvaluator::evaluate_extract_expression<std::string>(const ExtractExpression& extract_expression) {
//  const auto from_result = evaluate_expression<std::string>(*extract_expression.from());
//
//  switch (extract_expression.datetime_component) {
//    case DatetimeComponent::Year: return evaluate_extract_substr<0, 4>(from_result);
//    case DatetimeComponent::Month: return evaluate_extract_substr<5, 2>(from_result);
//    case DatetimeComponent::Day: return evaluate_extract_substr<8, 2>(from_result);
//
//    case DatetimeComponent::Hour:
//    case DatetimeComponent::Minute:
//    case DatetimeComponent::Second:
//      Fail("Hour, Minute and Second not available in String Datetimes");
//  }
//}

//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_extract_expression(const ExtractExpression& extract_expression) {
//  Fail("Only Strings (YYYY-MM-DD) supported for Dates right now");
//}
//
//template<size_t offset, size_t count>
//ExpressionResult<std::string> ExpressionEvaluator::evaluate_extract_substr(const ExpressionResult<std::string>& from_result) {
//  if (is_null(from_result)) {
//    return NullValue{};
//
//  } else if (is_value(from_result)) {
//    const auto& date = boost::get<std::string>(from_result);
//    DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
//    return date.substr(offset, count);
//
//  } else if (is_non_nullable_values(from_result)) {
//    const auto& from_values = boost::get<NonNullableValues<std::string>>(from_result);
//
//    auto result_values = NonNullableValues<std::string>(_output_row_count);
//
//    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
//      const auto& date = from_values[chunk_offset];
//      DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
//      result_values[chunk_offset] = date.substr(offset, count);
//    }
//
//    return result_values;
//
//  } else if (is_nullable_values(from_result)) {
//    const auto& from_values_and_nulls = boost::get<NullableValues<std::string>>(from_result);
//    const auto& from_values = from_values_and_nulls.first;
//    const auto& from_nulls = from_values_and_nulls.second;
//
//    auto result_values = std::vector<std::string>(_output_row_count);
//
//    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
//      const auto& date = from_values[chunk_offset];
//      DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
//      result_values[chunk_offset] = date.substr(offset, count);
//    }
//
//    return std::make_pair(result_values, from_nulls);
//
//  } else {
//    Fail("Can't EXTRACT from this Expression");
//  }
//}
//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_select_expression_for_chunk(
//const PQPSelectExpression &expression) {
//  for (const auto& parameter : expression.parameters) {
//    _ensure_column_materialization(parameter);
//  }
//
//  NonNullableValues<T> result;
//  result.reserve(_output_row_count);
//
//  std::vector<AllParameterVariant> parameter_values(expression.parameters.size());
//
//  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
//    const auto select_result = evaluate_select_expression_for_row<T>(expression, chunk_offset);
//    const auto& select_result_values = boost::get<NonNullableValues<T>>(select_result);
//
//    Assert(select_result_values.size() == 1, "Expected precisely one row");
//    result.emplace_back(select_result_values[0]);
//  }
//
//  return result;
//}
//
//template<typename T>
//ExpressionResult<T> ExpressionEvaluator::evaluate_select_expression_for_row(const PQPSelectExpression& expression, const ChunkOffset chunk_offset) {
//  Assert(expression.parameters.empty() || _chunk, "Sub-SELECT references external Columns but Expression doesn't operate on a Table/Chunk");
//
//  std::vector<AllParameterVariant> parameter_values(expression.parameters.size());
//
//  for (auto parameter_idx = size_t{0}; parameter_idx < expression.parameters.size(); ++parameter_idx) {
//    const auto parameter_column_id = expression.parameters[parameter_idx];
//    const auto& column = *_chunk->get_column(parameter_column_id);
//
//    resolve_data_type(column.data_type(), [&](const auto data_type_t) {
//      using ColumnDataType = typename decltype(data_type_t)::type;
//      parameter_values[parameter_idx] = AllTypeVariant{static_cast<ColumnMaterialization<ColumnDataType>&>(*_column_materializations[parameter_column_id]).values[chunk_offset]};
//    });
//  }
//
//  auto row_pqp = expression.pqp->recreate(parameter_values);
//
//  SQLQueryPlan query_plan;
//  query_plan.add_tree_by_root(row_pqp);
//  const auto tasks = query_plan.create_tasks();
//  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
//
//  const auto result_table = row_pqp->get_output();
//
//  Assert(result_table->column_count() == 1, "Expected precisely one column");
//  Assert(result_table->column_data_type(ColumnID{0}) == data_type_from_type<T>(), "Expected different DataType");
//
//  std::vector<T> result_values;
//  result_values.reserve(result_table->row_count());
//
//  for (auto chunk_id = ChunkID{0}; chunk_id < result_table->chunk_count(); ++chunk_id) {
//    const auto &result_column = *result_table->get_chunk(chunk_id)->get_column(ColumnID{0});
//    materialize_values(result_column, result_values);
//  }
//
//  return result_values;
//}

std::shared_ptr<BaseColumn> ExpressionEvaluator::evaluate_expression_to_column(const AbstractExpression& expression) {
  std::shared_ptr<BaseColumn> column;

  resolve_to_expression_result(expression, [&](const auto &expression_result) {
    using ColumnDataType = typename std::decay_t<decltype(expression_result)>::Type;

    // clang-format off
    if constexpr (std::is_same_v<ColumnDataType, NullValue>) {
      Fail("Can't create a Column from a NULLs and Arrays");
    } else {
      pmr_concurrent_vector<ColumnDataType> values(_output_row_count);

      if (expression_result.is_series()) {
        std::copy(expression_result.values.begin(), expression_result.values.end(), values.begin());
      } else {
        std::fill(values.begin(), values.end(), expression_result.values.front());
      }

      if (expression_result.is_nullable()) {
        pmr_concurrent_vector<bool> nulls(_output_row_count);

        if (expression_result.is_series()) {
          std::copy(expression_result.nulls.begin(), expression_result.nulls.end(), nulls.begin());
        } else {
          std::fill(nulls.begin(), nulls.end(), expression_result.nulls.front());
        }

        column = std::make_shared<ValueColumn<ColumnDataType>>(std::move(values), std::move(nulls));

      } else {
        column = std::make_shared<ValueColumn<ColumnDataType>>(std::move(values));
      }
    }
    // clang-format on
  });

  return column;
}

template<>
ExpressionResult<int32_t> ExpressionEvaluator::evaluate_logical_expression<int32_t>(const LogicalExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.logical_operator) {
    case LogicalOperator::Or:  return evaluate_binary_with_custom_null_logic<int32_t, TernaryOr>(left, right);
    case LogicalOperator::And: return evaluate_binary_with_custom_null_logic<int32_t, TernaryAnd>(left, right);
  }
  // clang-format on
}

//template<>
//ExpressionResult<int32_t> ExpressionEvaluator::evaluate_in_expression<int32_t>(const InExpression& in_expression) {
//  const auto& left_expression = *in_expression.value();
//  const auto& right_expression = *in_expression.set();
//
//  std::vector<int32_t> result_values;
//  std::vector<bool> result_nulls;
//
//  if (right_expression.type == ExpressionType::Array) {
//    const auto& array_expression = static_cast<const ArrayExpression&>(right_expression);
//
//    /**
//     * To keep the code simple for now, transform the InExpression like this:
//     * "a IN (x, y, z)"   ---->   "a = x OR a = y OR a = z"
//     *
//     * But first, out of array_expression.elements(), pick those expressions whose type can be compared with
//     * in_expression.value() so we're not getting "Can't compare Int and String" when doing something crazy like
//     * "5 IN (6, 5, "Hello")
//     */
//    const auto left_is_string = left_expression.data_type() == DataType::String;
//    std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
//    for (const auto& element : array_expression.elements()) {
//      if ((element->data_type() == DataType::String) == left_is_string) {
//        type_compatible_elements.emplace_back(element);
//      }
//    }
//
//    if (type_compatible_elements.empty()) {
//      // NULL IN () is NULL, <not_null> IN () is FALSE
//      Fail("Not supported yet");
//    }
//
//    std::shared_ptr<AbstractExpression> predicate_disjunction = equals(in_expression.value(), type_compatible_elements.front());
//    for (auto element_idx = size_t{1}; element_idx < type_compatible_elements.size(); ++element_idx) {
//      const auto equals_element = equals(in_expression.value(), type_compatible_elements[element_idx]);
//      predicate_disjunction = or_(predicate_disjunction, equals_element);
//    }
//
//    return evaluate_expression<int32_t>(*predicate_disjunction);
//  } else if (right_expression.type == ExpressionType::Select) {
//    Fail("Unsupported ExpressionType used in InExpression");
//  } else {
//    Fail("Unsupported ExpressionType used in InExpression");
//  }
//
//  return {};
//}


//template<typename T>
//std::vector<ExpressionResult<T>> ExpressionEvaluator::evaluate_array_expression(const ArrayExpression& array_expression) {
//  const auto element_data_type = array_expression.common_element_data_type();
//  Assert(element_data_type, "Can't evaluate Array with incompatible Element types")
//
//  std::vector<ExpressionResult<T>> result(array_expression.elements().size());
//
//  resolve_data_type(*element_data_type, [&](const auto element_data_type_t) {
//    using ElementDataType = typename decltype(element_data_type_t)::type;
//
//    for (auto element_idx = size_t{})
//  });
//}


template<typename R, typename Functor>
ExpressionResult<R> ExpressionEvaluator::evaluate_binary_with_default_null_logic(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression) {
  ExpressionResult<R> result;

  resolve_to_expression_results(left_expression, right_expression, [&](const auto &left, const auto &right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;


    if constexpr (Functor::template supports<R, LeftDataType, RightDataType>::value) {
      const auto result_size = _result_size(left, right);
      auto nulls = _evaluate_default_null_logic(left.nulls, right.nulls);

      std::vector<R> values(result_size);
      if (left.size() == right.size()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values[row_idx]);
        }
      } else if (left.size() > right.size()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values.front());
        }
      } else {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values.front(), right.values[row_idx]);
        }
      }

      result = ExpressionResult<R>{std::move(values), std::move(nulls)};
    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return result;
}

template<typename R, typename Functor>
ExpressionResult<R> ExpressionEvaluator::evaluate_binary_with_custom_null_logic(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression) {
  ExpressionResult<R> result;

  resolve_to_expression_result_views(left_expression, right_expression, [&](const auto &left, const auto &right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template supports<R, LeftDataType, RightDataType>::value) {
      const auto result_row_count = _result_size(left, right);

      std::vector<bool> nulls(result_row_count);
      std::vector<R> values(result_row_count);

      for (auto row_idx = ChunkOffset{0}; row_idx < result_row_count; ++row_idx) {
        bool null;
        Functor{}(values[row_idx], null, left.value(row_idx),
                  left.null(row_idx), right.value(row_idx),
                  right.null(row_idx));
        nulls[row_idx] = null;
      }

      result = ExpressionResult<R>{std::move(values), std::move(nulls)};

    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return result;
}

template<typename Functor>
void ExpressionEvaluator::resolve_to_expression_result_views(const AbstractExpression &left_expression,
                                                             const AbstractExpression &right_expression,
                                                             const Functor &fn) {
  resolve_to_expression_results(left_expression, right_expression, [&](const auto& left_result, const auto& right_result) {
    resolve_expression_result_to_view(left_result, [&](const auto &left_view) {
      resolve_expression_result_to_view(right_result, [&](const auto &right_view) {
        fn(left_view, right_view);
      });
    });
  });
}

template<typename Functor>
void ExpressionEvaluator::resolve_to_expression_results(const AbstractExpression &left_expression,
                                                             const AbstractExpression &right_expression,
                                                             const Functor &fn) {
  resolve_to_expression_result(left_expression, [&](const auto &left_result) {
    resolve_to_expression_result(right_expression, [&](const auto &right_result) {
      fn(left_result, right_result);
    });
  });
}

template<typename Functor>
void ExpressionEvaluator::resolve_to_expression_result(const AbstractExpression &expression, const Functor &fn) {
  Assert(expression.type != ExpressionType::Array, "Can't resolve ArrayExpression")
  Assert(expression.type != ExpressionType::Select, "Can't resolve SelectExpression")

  if (expression.data_type() == DataType::Null) {
    // resolve_data_type() doesn't support Null, so we have handle it explicitly
    ExpressionResult<NullValue> null_value_result{{NullValue{}}, {true}};

    fn(null_value_result);

  } else {
    resolve_data_type(expression.data_type(), [&] (const auto data_type_t) {
      using ExpressionDataType = typename decltype(data_type_t)::type;

      const auto expression_result = evaluate_expression<ExpressionDataType>(expression);
      fn(expression_result);
    });
  }
}

template<typename A, typename B>
ChunkOffset ExpressionEvaluator::_result_size(const A& a, const B& b) {
  return std::max(a.size(), b.size());
}

std::vector<bool> ExpressionEvaluator::_evaluate_default_null_logic(const std::vector<bool>& left,
                                                                           const std::vector<bool>& right) {
  DebugAssert(left.size() >= 1 && right.size() >= 1, "ExpressionEvaluator requires at least one row");

  const auto result_size = _result_size(left, right);


  if (left.size() == right.size()) {
    std::vector<bool> nulls(result_size);
    std::transform(left.begin(), left.end(), right.begin(), nulls.begin(), [](auto l, auto r) { return l || r; });
    return nulls;
  } else if (left.size() > right.size()) {
    DebugAssert(right.size() == 1, "Operand should have either the same row as the other or 1, to represent a literal");
    if (right.front()) return std::vector<bool>({true});
    else return left;
  } else {
    DebugAssert(left.size() == 1, "Operand should have either the same row as the other or 1, to represent a literal");
    if (left.front()) return std::vector<bool>({true});
    else return right;
  }
}

}  // namespace opossum