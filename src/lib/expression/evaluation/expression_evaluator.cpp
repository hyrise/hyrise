#include "expression_evaluator.hpp"

#include <iterator>
#include <type_traits>

#include "boost/variant/apply_visitor.hpp"

#include "all_parameter_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression_functors.hpp"
#include "like_matcher.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/materialize.hpp"
#include "storage/value_column.hpp"
#include "utils/assert.hpp"
#include "utils/lambda_visitor.hpp"

using namespace std::string_literals;
using namespace opossum::expression_factory;

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Table>& table, const ChunkID chunk_id)
    : _table(table), _chunk(_table->get_chunk(chunk_id)) {
  _output_row_count = _chunk->size();
  _column_materializations.resize(_chunk->column_count());
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::evaluate_expression_to_result(
    const AbstractExpression& expression) {
  switch (expression.type) {
    case ExpressionType::Arithmetic:
      return _evaluate_arithmetic_expression<R>(static_cast<const ArithmeticExpression&>(expression));

    case ExpressionType::Logical:
      return _evaluate_logical_expression<R>(static_cast<const LogicalExpression&>(expression));

    case ExpressionType::Predicate:
      return _evaluate_predicate_expression<R>(static_cast<const AbstractPredicateExpression&>(expression));

    case ExpressionType::Select: {
      const auto* pqp_select_expression = dynamic_cast<const PQPSelectExpression*>(&expression);
      Assert(pqp_select_expression, "Can only evaluate PQPSelectExpression, LQPSelectExpressions need to be translated first");
      return _evaluate_select_expression<R>(*pqp_select_expression);
    }

    case ExpressionType::Column: {
      const auto* pqp_column_expression = dynamic_cast<const PQPColumnExpression*>(&expression);
      Assert(pqp_column_expression, "Can only evaluate PQPColumnExpressions, LQPSelectExpressions need to be translated first");
      return _evaluate_column_expression<R>(*pqp_column_expression);
    }

    // ValueExpression and ParameterExpression both need to unpack an AllTypeVariant, so one functions handles both
    case ExpressionType::Parameter:
    case ExpressionType::Value:
      return _evaluate_value_or_parameter_expression<R>(expression);

    case ExpressionType::Function:
      return _evaluate_function_expression<R>(static_cast<const FunctionExpression&>(expression));

    case ExpressionType::Case:
      return _evaluate_case_expression<R>(static_cast<const CaseExpression&>(expression));

    case ExpressionType::Cast:
      return _evaluate_cast_expression<R>(static_cast<const CastExpression&>(expression));

    case ExpressionType::Exists:
      return _evaluate_exists_expression<R>(static_cast<const ExistsExpression&>(expression));

    case ExpressionType::Extract:
      return _evaluate_extract_expression<R>(static_cast<const ExtractExpression&>(expression));

    case ExpressionType::Negate:
      return _evaluate_negate_expression<R>(static_cast<const NegateExpression&>(expression));

    case ExpressionType::Aggregate:
      Fail("ExpressionEvaluator doesn't support Aggregates, use the Aggregate Operator to compute them");

    case ExpressionType::List:
      Fail("Can't evaluate a ListExpression, lists should only appear as the right operand of an InExpression");
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_arithmetic_expression(
    const ArithmeticExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:       return _evaluate_binary_with_default_null_logic<R, Addition>(left, right);
    case ArithmeticOperator::Subtraction:    return _evaluate_binary_with_default_null_logic<R, Subtraction>(left, right);  // NOLINT
    case ArithmeticOperator::Multiplication: return _evaluate_binary_with_default_null_logic<R, Multiplication>(left, right);  // NOLINT

    // Division and Modulo need to catch division by zero
    case ArithmeticOperator::Division:       return _evaluate_binary_with_custom_null_logic<R, Division>(left, right);
    case ArithmeticOperator::Modulo:         return _evaluate_binary_with_custom_null_logic<R, Modulo>(left, right);
  }
  // clang-format on
}
template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_binary_predicate_expression<int32_t>(
    const BinaryPredicateExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.predicate_condition) {
    case PredicateCondition::Equals:            return _evaluate_binary_with_default_null_logic<int32_t, Equals>(left, right);  // NOLINT
    case PredicateCondition::NotEquals:         return _evaluate_binary_with_default_null_logic<int32_t, NotEquals>(left, right);  // NOLINT
    case PredicateCondition::LessThan:          return _evaluate_binary_with_default_null_logic<int32_t, LessThan>(left, right);  // NOLINT
    case PredicateCondition::LessThanEquals:    return _evaluate_binary_with_default_null_logic<int32_t, LessThanEquals>(left, right);  // NOLINT
    case PredicateCondition::GreaterThan:       return _evaluate_binary_with_default_null_logic<int32_t, GreaterThan>(left, right);  // NOLINT
    case PredicateCondition::GreaterThanEquals: return _evaluate_binary_with_default_null_logic<int32_t, GreaterThanEquals>(left, right);  // NOLINT

    default:
      Fail("PredicateCondition should be handled in different function");
  }
  // clang-format on
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_binary_predicate_expression(
    const BinaryPredicateExpression& expression) {
  Fail("Can only evaluate predicates to int32_t (aka bool)");
}

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_like_expression<int32_t>(
    const BinaryPredicateExpression& expression) {
  /**
   * NOTE: This code path is NOT taken for LIKEs in predicates. That is `SELECT * FROM t WHERE a LIKE '%Hello%'` is
   *        handled in the TableScan. This code path is for `SELECT a LIKE 'bla' FROM ...` and alike
   */

  Assert(expression.predicate_condition == PredicateCondition::Like ||
             expression.predicate_condition == PredicateCondition::NotLike,
         "Expected PredicateCondition Like or NotLike");

  const auto left_results = evaluate_expression_to_result<std::string>(*expression.left_operand());
  const auto right_results = evaluate_expression_to_result<std::string>(*expression.right_operand());

  const auto invert_results = expression.predicate_condition == PredicateCondition::NotLike;

  const auto result_size = _result_size(left_results->size(), right_results->size());
  auto result_values = std::vector<int32_t>(result_size, 0);

  /**
   * Three different kinds of LIKE are considered for performance reasons and avoid redundant creation of the
   * LikeMatcher
   *    - `a LIKE b`
   *    - `a LIKE '%hello%'`
   *    - `'hello' LIKE b`
   */
  if (left_results->is_literal() == right_results->is_literal()) {
    // E.g., `a LIKE b` - A new matcher for each row and a different value as well
    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      LikeMatcher{right_results->values[row_idx]}.resolve(invert_results, [&](const auto& matcher) {
        result_values[row_idx] = matcher(left_results->values[row_idx]);
      });
    }
  } else if (!left_results->is_literal() && right_results->is_literal()) {
    // E.g., `a LIKE '%hello%'` -- A single matcher for all rows
    LikeMatcher like_matcher{right_results->values.front()};

    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      like_matcher.resolve(invert_results, [&](const auto& matcher) {
        result_values[row_idx] = matcher(left_results->values[row_idx]);
      });
    }
  } else {
    // E.g., `'hello' LIKE b` -- A new matcher for each row but the value to check is constant
    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      LikeMatcher{right_results->values[row_idx]}.resolve(
          invert_results, [&](const auto& matcher) { result_values[row_idx] = matcher(left_results->values.front()); });
    }
  }

  auto result_nulls = _evaluate_default_null_logic(left_results->nulls, right_results->nulls);

  return std::make_shared<ExpressionResult<int32_t>>(std::move(result_values), std::move(result_nulls));
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_like_expression(
    const BinaryPredicateExpression& expression) {
  Fail("Can only evaluate predicates to int32_t (aka bool)");
}

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_is_null_expression<int32_t>(
    const IsNullExpression& expression) {
  std::vector<int32_t> result_values;

  _resolve_to_expression_result_view(*expression.operand(), [&](const auto& view) {
    result_values.resize(view.size());

    if (expression.predicate_condition == PredicateCondition::IsNull) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < view.size(); ++chunk_offset) {
        result_values[chunk_offset] = view.null(chunk_offset);
      }
    } else {  // PredicateCondition::IsNotNull
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < view.size(); ++chunk_offset) {
        result_values[chunk_offset] = !view.null(chunk_offset);
      }
    }
  });

  return std::make_shared<ExpressionResult<int32_t>>(std::move(result_values));
}

template <typename T>
std::shared_ptr<ExpressionResult<T>> ExpressionEvaluator::_evaluate_is_null_expression(
    const IsNullExpression& expression) {
  Fail("Can only evaluate predicates to int32_t (aka bool)");
}

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_in_expression<int32_t>(
    const InExpression& in_expression) {
  const auto& left_expression = *in_expression.value();
  const auto& right_expression = *in_expression.set();

  std::vector<int32_t> result_values;
  std::vector<bool> result_nulls;

  if (right_expression.type == ExpressionType::List) {
    const auto& array_expression = static_cast<const ListExpression&>(right_expression);

    /**
     * To keep the code simple for now, transform the InExpression like this:
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
      // `5 IN ()` is FALSE as is `NULL IN ()`
      return std::make_shared<ExpressionResult<int32_t>>(std::vector<int32_t>{0});
    }

    std::shared_ptr<AbstractExpression> predicate_disjunction =
        equals(in_expression.value(), type_compatible_elements.front());
    for (auto element_idx = size_t{1}; element_idx < type_compatible_elements.size(); ++element_idx) {
      const auto equals_element = equals(in_expression.value(), type_compatible_elements[element_idx]);
      predicate_disjunction = or_(predicate_disjunction, equals_element);
    }

    return evaluate_expression_to_result<int32_t>(*predicate_disjunction);

  } else if (right_expression.type == ExpressionType::Select) {
    const auto* select_expression = dynamic_cast<const PQPSelectExpression*>(&right_expression);
    Assert(select_expression, "Expected PQPSelectExpression");

    resolve_data_type(select_expression->data_type(), [&](const auto select_data_type_t) {
      using SelectDataType = typename decltype(select_data_type_t)::type;

      const auto select_result_tables = _evaluate_select_expression_to_tables(*select_expression);
      const auto select_result_columns = _prune_tables_to_expression_results<SelectDataType>(select_result_tables);

      Assert(select_result_columns.size() == 1 || select_result_columns.size() == _output_row_count,
             "Unexpected number of lists returned from Select. "
             "Should be one (if the Select is uncorrelated), or one per row (if it is)");

      _resolve_to_expression_result_view(left_expression, [&](const auto& left_view) {
        using ValueDataType = typename std::decay_t<decltype(left_view)>::Type;

        if constexpr (Equals::supports<int32_t, ValueDataType, SelectDataType>::value) {
          const auto result_size = _result_size(left_view.size(), select_result_columns.size());

          result_values.resize(result_size, 0);
          // TODO(moritz) The InExpression doesn't in all cases need to return a nullable
          result_nulls.resize(result_size);

          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < result_size; ++chunk_offset) {
            // If the SELECT returned just one list, always perform the IN check with that one list
            // If the SELECT returned multiple lists, then the Select was correlated and we need to do the IN check
            // against the list of the current row
            const auto& list = *select_result_columns[select_result_columns.size() == 1 ? 0 : chunk_offset];

            auto list_contains_null = false;

            for (auto list_element_idx = ChunkOffset{0}; list_element_idx < list.size(); ++list_element_idx) {
              // `a IN (x,y,z)` is supposed to have the same semantics as `a = x OR a = y OR a = z`, so we use `Equals`
              // here as well.
              Equals{}(result_values[chunk_offset], list.value(list_element_idx), left_view.value(chunk_offset));
              if (result_values[chunk_offset]) break;

              list_contains_null |= list.null(list_element_idx);
            }

            result_nulls[chunk_offset] =
                (result_values[chunk_offset] == 0 && list_contains_null) || left_view.null(chunk_offset);
          }

        } else {
          // Tried to do, e.g., `5 IN (<select_returning_string>)` - return false instead of failing, because that's
          // what we do for `5 IN ('Hello', 'World')
          result_values.resize(1, 0);
        }
      });
    });

  } else {
    Fail("Unsupported ExpressionType used in InExpression");
  }

  if (result_nulls.empty()) {
    return std::make_shared<ExpressionResult<int32_t>>(std::move(result_values));
  } else {
    return std::make_shared<ExpressionResult<int32_t>>(std::move(result_values), std::move(result_nulls));
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_in_expression(const InExpression& expression) {
  Fail("InExpression supports only int32_t as result");
}

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_predicate_expression<int32_t>(
    const AbstractPredicateExpression& predicate_expression) {
  switch (predicate_expression.predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
      return _evaluate_binary_predicate_expression<int32_t>(
          static_cast<const BinaryPredicateExpression&>(predicate_expression));

    case PredicateCondition::Between: {
      // `a BETWEEN b AND c` is evaluated by transforming it to `a >= b AND a <= c` instead of evaluating it with a
      // dedicated algorithm. This is because three expression data types (from three arguments) generate many type
      // combinations and thus lengthen compile time and increase binary size notably.

      const auto& between_expression = static_cast<const BetweenExpression&>(predicate_expression);
      const auto gte_expression = greater_than_equals(between_expression.value(), between_expression.lower_bound());
      const auto lte_expression = less_than_equals(between_expression.value(), between_expression.upper_bound());

      const auto gte_lte_expression = and_(gte_expression, lte_expression);

      return evaluate_expression_to_result<int32_t>(*gte_lte_expression);
    }

    case PredicateCondition::In:
      return _evaluate_in_expression<int32_t>(static_cast<const InExpression&>(predicate_expression));

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return _evaluate_like_expression<int32_t>(static_cast<const BinaryPredicateExpression&>(predicate_expression));

    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      return _evaluate_is_null_expression<int32_t>(static_cast<const IsNullExpression&>(predicate_expression));
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_predicate_expression(
    const AbstractPredicateExpression& expression) {
  Fail("Can only evaluate predicates to int32_t (aka bool)");
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_column_expression(
    const PQPColumnExpression& column_expression) {
  Assert(_chunk, "Cannot access Columns in this Expression as it doesn't operate on a Table/Chunk");

  const auto& column = *_chunk->get_column(column_expression.column_id);
  Assert(column.data_type() == data_type_from_type<R>(), "Can't evaluate column to different type");

  _ensure_column_materialization(column_expression.column_id);
  return std::static_pointer_cast<ExpressionResult<R>>(_column_materializations[column_expression.column_id]);
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_case_expression(
    const CaseExpression& case_expression) {
  const auto when = evaluate_expression_to_result<int32_t>(*case_expression.when());

  std::shared_ptr<ExpressionResult<R>> result;

  _resolve_to_expression_results(
    *case_expression.then(), *case_expression.else_(), [&](const auto& then_result, const auto& else_result) {
      using ThenResultType = typename std::decay_t<decltype(then_result)>::Type;
      using ElseResultType = typename std::decay_t<decltype(else_result)>::Type;

      const auto result_size = _result_size(when->size(), then_result.size(), else_result.size());
      std::vector<R> values(result_size);
      std::vector<bool> nulls(result_size);

      // clang-format off
      if constexpr (Case::template supports<R, ThenResultType, ElseResultType>::value) {
        for (auto chunk_offset = ChunkOffset{0};
             chunk_offset < result_size; ++chunk_offset) {
          if (when->value(chunk_offset) && !when->null(chunk_offset)) {
            values[chunk_offset] = to_value<R>(then_result.value(chunk_offset));
            nulls[chunk_offset] = then_result.null(chunk_offset);
          } else {
            values[chunk_offset] = to_value<R>(else_result.value(chunk_offset));
            nulls[chunk_offset] = else_result.null(chunk_offset);
          }
        }
      } else {
        Fail("Illegal operands for CaseExpression");
      }
      // clang-format on

      result = std::make_shared<ExpressionResult<R>>(std::move(values), std::move(nulls));
    });

  return result;
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_cast_expression(const CastExpression& cast_expression) {
  /**
   * Implements SQL's CAST with the following semantics
   *    Float/Double -> Int/Long:           Value gets floor()ed
   *    String -> Int/Long/Float/Double:    Conversion is attempted, on error zero is returned
   *                                        in accordance with SQLite. (" 5hallo" AS INT) -> 5
   *    NULL -> Any type                    A nulled value of the requested type is returned.
   */

  auto values = std::vector<R>{};
  auto nulls = std::vector<bool>{};

  _resolve_to_expression_result(*cast_expression.argument(), [&](const auto& argument_result) {
    using ArgumentDataType = typename std::decay_t<decltype(argument_result)>::Type;

    const auto result_size = _result_size(argument_result.size());

    values.resize(result_size);

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < result_size; ++chunk_offset) {
      const auto& argument_value = argument_result.value(chunk_offset);

      if constexpr (std::is_same_v<R, NullValue> || std::is_same_v<ArgumentDataType, NullValue>) {
        // Something to Null cast. Do nothing, this is handled by the nulls vector
      } else if constexpr (std::is_same_v<R, std::string>) {
        if constexpr(std::is_same_v<ArgumentDataType, std::string>) {
          // String to String "cast"
          values[chunk_offset] = argument_value;
        } else {
          // Numeric/NULL to String cast
          values[chunk_offset] = std::to_string(argument_value);
        }
      } else {
        if constexpr(std::is_same_v<ArgumentDataType, std::string>) {
          // String to Numeric cast. Uses sto{l/d}
          if constexpr(std::is_same_v<R, int32_t> || std::is_same_v<R, int64_t>) {
            values[chunk_offset] = std::stol(argument_value);
          } else if constexpr(std::is_same_v<R, float> || std::is_same_v<R, double>) {
            values[chunk_offset] = std::stod(argument_value);
          } else {
            Fail("Casting string to numeric argument type not implemented");
          }
        } else {
          // Numeric to Numeric cast
          values[chunk_offset] = static_cast<R>(argument_value);
        }
      }
    }

    nulls = argument_result.nulls;
  });

  return std::make_shared<ExpressionResult<R>>(std::move(values), std::move(nulls));
}

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_exists_expression<int32_t>(
    const ExistsExpression& exists_expression) {
  const auto select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(exists_expression.select());
  Assert(select_expression, "Expected PQPSelectExpression");

  const auto select_result_tables = _evaluate_select_expression_to_tables(*select_expression);

  std::vector<int32_t> result_values(select_result_tables.size());
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < select_result_tables.size(); ++chunk_offset) {
    result_values[chunk_offset] = select_result_tables[chunk_offset]->row_count() > 0;
  }

  return std::make_shared<ExpressionResult<int32_t>>(std::move(result_values));
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_exists_expression(
    const ExistsExpression& exists_expression) {
  Fail("Exists can only return int32_t");
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_value_or_parameter_expression(
    const AbstractExpression& expression) {
  AllTypeVariant value;

  if (expression.type == ExpressionType::Value) {
    const auto& value_expression = static_cast<const ValueExpression&>(expression);
    value = value_expression.value;
  } else {
    const auto& parameter_expression = static_cast<const ParameterExpression&>(expression);
    Assert(parameter_expression.value().has_value(), "ParameterExpression: Parameter not set, cannot evaluate");
    value = *parameter_expression.value();
  }

  if (value.type() == typeid(NullValue)) {
    // NullValue can be evaluated to any type - it is then a null value of that type.
    // This makes it easier to implement expressions where a certain data type is expected, but a Null literal is
    // given. Think `CASE NULL THEN ... ELSE ...` - the NULL will be evaluated to be a bool.
    std::vector<bool> nulls{};
    nulls.emplace_back(true);
    return std::make_shared<ExpressionResult<R>>(std::vector<R>{{R{}}}, nulls);
  } else {
    Assert(value.type() == typeid(R), "Can't evaluate ValueExpression to requested type R");
    return std::make_shared<ExpressionResult<R>>(std::vector<R>{{boost::get<R>(value)}});
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_function_expression(
    const FunctionExpression& expression) {
  switch (expression.function_type) {
    case FunctionType::Concatenate:
    case FunctionType::Substring:
      // clang-format off
      if constexpr (std::is_same_v<R, std::string>) {
        switch (expression.function_type) {
          case FunctionType::Substring: return _evaluate_substring(expression.arguments);
          case FunctionType::Concatenate: return _evaluate_concatenate(expression.arguments);
        }
      } else {
        Fail("Function can only be evaluated to a string");
      }
      // clang-format on
  }
}

template <>
std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::_evaluate_extract_expression<std::string>(
    const ExtractExpression& extract_expression) {
  const auto from_result = evaluate_expression_to_result<std::string>(*extract_expression.from());

  switch (extract_expression.datetime_component) {
    case DatetimeComponent::Year:
      return _evaluate_extract_substr<0, 4>(*from_result);
    case DatetimeComponent::Month:
      return _evaluate_extract_substr<5, 2>(*from_result);
    case DatetimeComponent::Day:
      return _evaluate_extract_substr<8, 2>(*from_result);

    case DatetimeComponent::Hour:
    case DatetimeComponent::Minute:
    case DatetimeComponent::Second:
      Fail("Hour, Minute and Second not available in String Datetimes");
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_extract_expression(
    const ExtractExpression& extract_expression) {
  Fail("Only Strings (YYYY-MM-DD) supported for Dates right now");
}

template <size_t offset, size_t count>
std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::_evaluate_extract_substr(
    const ExpressionResult<std::string>& from_result) {
  std::shared_ptr<ExpressionResult<std::string>> result;

  std::vector<std::string> values(from_result.size());

  from_result.as_view([&](const auto& from_view) {
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < from_view.size(); ++chunk_offset) {
      if (!from_view.null(chunk_offset)) {
        DebugAssert(from_view.value(chunk_offset).size() == 10u,
                    "Invalid DatetimeString '"s + from_view.value(chunk_offset) + "'");
        values[chunk_offset] = from_view.value(chunk_offset).substr(offset, count);
      }
    }
  });

  return std::make_shared<ExpressionResult<std::string>>(std::move(values), from_result.nulls);
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_negate_expression(
    const NegateExpression& negate_expression) {
  std::vector<R> values;
  std::vector<bool> nulls;

  _resolve_to_expression_result(*negate_expression.argument(), [&](const auto& argument_result) {
    using ArgumentType = typename std::decay_t<decltype(argument_result)>::Type;

    // clang-format off
    if constexpr (!std::is_same_v<ArgumentType, std::string> && std::is_same_v<R, ArgumentType>) {
      values.resize(argument_result.size());
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < argument_result.size(); ++chunk_offset) {
        // NOTE: Actual negation happens in this line
        values[chunk_offset] = -argument_result.values[chunk_offset];
      }
      nulls = argument_result.nulls;
    } else {
      Fail("Can't negate a Strings, can't negate an argument to a different type");
    }
    // clang-format on
  });

  if (nulls.empty()) {
    return std::make_shared<ExpressionResult<R>>(std::move(values));
  } else {
    return std::make_shared<ExpressionResult<R>>(std::move(values), std::move(nulls));
  }
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_select_expression(
    const PQPSelectExpression& select_expression) {
  const auto select_result_tables = _evaluate_select_expression_to_tables(select_expression);

  // The single columns returned from invoking the SelectExpression on each row. So: one column per row.
  const auto select_result_columns = _prune_tables_to_expression_results<R>(select_result_tables);

  std::vector<R> result_values(select_result_columns.size());

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < select_result_columns.size(); ++chunk_offset) {
    Assert(select_result_columns[chunk_offset]->size() == 1,
           "Expected precisely one row to be returned from SelectExpression");
    result_values[chunk_offset] = select_result_columns[chunk_offset]->value(0);
  }

  if (select_expression.is_nullable()) {
    std::vector<bool> result_nulls(select_result_columns.size());

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < select_result_columns.size(); ++chunk_offset) {
      result_nulls[chunk_offset] = select_result_columns[chunk_offset]->null(0);
    }
    return std::make_shared<ExpressionResult<R>>(std::move(result_values), std::move(result_nulls));
  } else {
    return std::make_shared<ExpressionResult<R>>(std::move(result_values));
  }
}

std::vector<std::shared_ptr<const Table>> ExpressionEvaluator::_evaluate_select_expression_to_tables(
    const PQPSelectExpression& expression) {
  // If the SelectExpression is uncorrelated, evaluating it once is sufficient
  if (expression.parameters.empty()) {
    return {_evaluate_select_expression_for_row(expression, ChunkOffset{0})};
  }

  // Make sure all Columns that are parameters are materialized
  for (const auto& parameter : expression.parameters) {
    _ensure_column_materialization(parameter.second);
  }

  std::vector<std::shared_ptr<const Table>> results(_output_row_count);

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
    results[chunk_offset] = _evaluate_select_expression_for_row(expression, chunk_offset);
  }

  return results;
}

std::shared_ptr<const Table> ExpressionEvaluator::_evaluate_select_expression_for_row(
    const PQPSelectExpression& expression, const ChunkOffset chunk_offset) {
  Assert(expression.parameters.empty() || _chunk,
         "Sub-SELECT references external Columns but Expression doesn't operate on a Table/Chunk");

  std::unordered_map<ParameterID, AllTypeVariant> parameters;

  for (auto parameter_idx = size_t{0}; parameter_idx < expression.parameters.size(); ++parameter_idx) {
    const auto& parameter_id_column_id = expression.parameters[parameter_idx];
    const auto parameter_id = parameter_id_column_id.first;
    const auto column_id = parameter_id_column_id.second;
    const auto& column = *_chunk->get_column(column_id);

    resolve_data_type(column.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto column_materialization =
          std::dynamic_pointer_cast<ExpressionResult<ColumnDataType>>(_column_materializations[column_id]);

      if (column_materialization->null(chunk_offset)) {
        parameters.emplace(parameter_id, NullValue{});
      } else {
        parameters.emplace(parameter_id, column_materialization->value(chunk_offset));
      }
    });
  }

  // TODO(moritz) recreate() shouldn't be necessary for every row if we could re-execute PQPs...
  auto row_pqp = expression.pqp->recreate();
  row_pqp->set_parameters(parameters);

  SQLQueryPlan query_plan{CleanupTemporaries::Yes};
  query_plan.add_tree_by_root(row_pqp);
  const auto tasks = query_plan.create_tasks();
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  return row_pqp->get_output();
}

std::shared_ptr<BaseColumn> ExpressionEvaluator::evaluate_expression_to_column(const AbstractExpression& expression) {
  std::shared_ptr<BaseColumn> column;

  _resolve_to_expression_result_view(expression, [&](const auto& view) {
    using ColumnDataType = typename std::decay_t<decltype(view)>::Type;

    // clang-format off
    if constexpr (std::is_same_v<ColumnDataType, NullValue>) {
      Fail("Can't create a Column from a NULL");
    } else {
      pmr_concurrent_vector<ColumnDataType> values(_output_row_count);

      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
        values[chunk_offset] = view.value(chunk_offset);
      }

      if (view.is_nullable()) {
        pmr_concurrent_vector<bool> nulls(_output_row_count);
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
          nulls[chunk_offset] = view.null(chunk_offset);
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

template <>
std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::_evaluate_logical_expression<int32_t>(
    const LogicalExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.logical_operator) {
    case LogicalOperator::Or:  return _evaluate_binary_with_custom_null_logic<int32_t, TernaryOr>(left, right);
    case LogicalOperator::And: return _evaluate_binary_with_custom_null_logic<int32_t, TernaryAnd>(left, right);
  }
  // clang-format on
}

template <typename R>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_logical_expression(
    const LogicalExpression& expression) {
  Fail("LogicalExpression can only output int32_t");
}

template <typename R, typename Functor>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_binary_with_default_null_logic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  auto result = std::shared_ptr<ExpressionResult<R>>{};

  _resolve_to_expression_results(left_expression, right_expression, [&](const auto& left, const auto& right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template supports<R, LeftDataType, RightDataType>::value) {
      const auto result_size = _result_size(left.size(), right.size());
      auto nulls = _evaluate_default_null_logic(left.nulls, right.nulls);

      // Using three different branches instead of views, which would generate 9 cases.
      std::vector<R> values(result_size);
      if (left.is_literal() == right.is_literal()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values[row_idx]);
        }
      } else if (right.is_literal()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values.front());
        }
      } else {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values.front(), right.values[row_idx]);
        }
      }

      result = std::make_shared<ExpressionResult<R>>(std::move(values), std::move(nulls));
    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return result;
}

template <typename R, typename Functor>
std::shared_ptr<ExpressionResult<R>> ExpressionEvaluator::_evaluate_binary_with_custom_null_logic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  auto result = std::shared_ptr<ExpressionResult<R>>{};

  _resolve_to_expression_result_views(left_expression, right_expression, [&](const auto& left, const auto& right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template supports<R, LeftDataType, RightDataType>::value) {
      const auto result_row_count = _result_size(left.size(), right.size());

      std::vector<bool> nulls(result_row_count);
      std::vector<R> values(result_row_count);

      for (auto row_idx = ChunkOffset{0}; row_idx < result_row_count; ++row_idx) {
        bool null;
        Functor{}(values[row_idx], null, left.value(row_idx), left.null(row_idx), right.value(row_idx),
                  right.null(row_idx));
        nulls[row_idx] = null;
      }

      result = std::make_shared<ExpressionResult<R>>(std::move(values), std::move(nulls));

    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return result;
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result_view(const AbstractExpression& expression, const Functor& fn) {
  _resolve_to_expression_result(expression,
                                [&](const auto& result) { result.as_view([&](const auto& view) { fn(view); }); });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result_views(const AbstractExpression& left_expression,
                                                              const AbstractExpression& right_expression,
                                                              const Functor& fn) {
  _resolve_to_expression_results(left_expression, right_expression,
                                 [&](const auto& left_result, const auto& right_result) {
                                   left_result.as_view([&](const auto& left_view) {
                                     right_result.as_view([&](const auto& right_view) { fn(left_view, right_view); });
                                   });
                                 });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_results(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression,
                                                         const Functor& fn) {
  _resolve_to_expression_result(left_expression, [&](const auto& left_result) {
    _resolve_to_expression_result(right_expression, [&](const auto& right_result) { fn(left_result, right_result); });
  });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result(const AbstractExpression& expression, const Functor& fn) {
  Assert(expression.type != ExpressionType::List, "Can't resolve ListExpression to ExpressionResult");

  if (expression.data_type() == DataType::Null) {
    // resolve_data_type() doesn't support Null, so we have handle it explicitly
    ExpressionResult<NullValue> null_value_result{{NullValue{}}, {true}};

    fn(null_value_result);

  } else {
    resolve_data_type(expression.data_type(), [&](const auto data_type_t) {
      using ExpressionDataType = typename decltype(data_type_t)::type;

      const auto expression_result = evaluate_expression_to_result<ExpressionDataType>(expression);
      fn(*expression_result);
    });
  }
}

template <typename... RowCounts>
ChunkOffset ExpressionEvaluator::_result_size(const RowCounts... row_counts) {
  // If any operand is empty (that's the case IFF it is an empty column) the result of the expression has no rows
  if (((row_counts == 0) || ...)) return 0;

  return std::max({row_counts...});
}

std::vector<bool> ExpressionEvaluator::_evaluate_default_null_logic(const std::vector<bool>& left,
                                                                    const std::vector<bool>& right) const {
  const auto result_size = _result_size(left.size(), right.size());

  if (result_size == 0) return {};

  if (left.size() == right.size()) {
    std::vector<bool> nulls(result_size);
    std::transform(left.begin(), left.end(), right.begin(), nulls.begin(), [](auto l, auto r) { return l || r; });
    return nulls;
  } else if (left.size() > right.size()) {
    DebugAssert(right.size() == 1,
                "Operand should have either the same row count as the other or 1 (to represent a literal)");
    if (right.front())
      return std::vector<bool>({true});
    else
      return left;
  } else {
    DebugAssert(left.size() == 1,
                "Operand should have either the same row count as the other or 1 (to represent a literal)");
    if (left.front())
      return std::vector<bool>({true});
    else
      return right;
  }
}

void ExpressionEvaluator::_ensure_column_materialization(const ColumnID column_id) {
  Assert(_chunk, "Cannot access Columns in this Expression as it doesn't operate on a Table/Chunk");

  if (_column_materializations[column_id]) return;

  const auto& column = *_chunk->get_column(column_id);

  resolve_data_type(column.data_type(), [&](const auto column_data_type_t) {
    using ColumnDataType = typename decltype(column_data_type_t)::type;

    std::vector<ColumnDataType> values;
    materialize_values(column, values);

    if (_table->column_is_nullable(column_id)) {
      std::vector<bool> nulls;
      materialize_nulls<ColumnDataType>(column, nulls);
      _column_materializations[column_id] =
          std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values), std::move(nulls));

    } else {
      _column_materializations[column_id] = std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values));
    }
  });
}

std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::_evaluate_substring(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  DebugAssert(arguments.size() == 3, "SUBSTR expects three arguments");

  const auto strings = evaluate_expression_to_result<std::string>(*arguments[0]);
  const auto starts = evaluate_expression_to_result<int32_t>(*arguments[1]);
  const auto lengths = evaluate_expression_to_result<int32_t>(*arguments[2]);

  const auto row_count = _result_size(strings->size(), starts->size(), lengths->size());

  std::vector<std::string> result_values(row_count);
  std::vector<bool> result_nulls(row_count);

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
    result_nulls[chunk_offset] =
        strings->null(chunk_offset) || starts->null(chunk_offset) || lengths->null(chunk_offset);

    const auto& string = strings->value(chunk_offset);
    DebugAssert(string.size() < std::numeric_limits<int32_t>::max(),
           "String is too long to be handled by SUBSTR. Switch to int64_t in the SUBSTR implementation if you really "
           "need to.");

    const auto signed_string_size = static_cast<int32_t>(string.size());

    auto length = lengths->value(chunk_offset);
    if (length <= 0) continue;

    auto start = starts->value(chunk_offset);

    /**
     * Hyrise SUBSTR follows SQLite semantics for negative indices. SUBSTR lives in this weird "space" illustrated below
     * Note that other DBMS behave differently when it comes to negative indices.
     *
     * START -8 -7 -6 -5 -4 -3 -2 -1 || 0  || 1 2 3 4 5 6  7  8
     * CHAR  // // // H  e  l  l  o  || // || H e l l o // // //
     *
     * SUBSTR('HELLO', 0, 2) -> 'H'
     * SUBSTR('HELLO', -1, 2) -> 'O'
     * SUBSTR('HELLO', -8, 1) -> ''
     * SUBSTR('HELLO', -8, 5) -> 'HE'
     */
    auto end = int32_t{0};
    if (start < 0) {
      start += signed_string_size;
    } else {
      if (start == 0) {
        length -= 1;
      } else {
        start -= 1;
      }
    }

    end = start + length;
    start = std::max(0, start);
    end = std::min(end, signed_string_size);
    length = end - start;

    // Invalid/out of range arguments, like SUBSTR("HELLO", 4000, -2), lead to an empty string
    if (!string.empty() && start >= 0 && start < signed_string_size && length > 0) {
      length = std::min<int32_t>(signed_string_size - start, length);
      result_values[chunk_offset] = string.substr(static_cast<size_t>(start), static_cast<size_t>(length));
    }
  }

  return std::make_shared<ExpressionResult<std::string>>(result_values, result_nulls);
}

std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::_evaluate_concatenate(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  std::vector<std::shared_ptr<ExpressionResult<std::string>>> argument_results;
  argument_results.reserve(arguments.size());

  auto result_is_nullable = false;

  // 1 - Compute the arguments
  for (const auto& argument : arguments) {
    // CONCAT with a NULL literal argument -> result is NULL
    if (argument->data_type() == DataType::Null) {
      auto null_value_result = ExpressionResult<std::string>{{{}}, {true}};
      return std::make_shared<ExpressionResult<std::string>>(null_value_result);
    }

    const auto argument_result = evaluate_expression_to_result<std::string>(*argument);
    argument_results.emplace_back(argument_result);

    result_is_nullable |= argument_result->is_nullable();
  }

  // 2 - Compute the number of output rows
  auto result_size = argument_results.empty() ? size_t{0} : argument_results.front()->size();
  for (auto argument_idx = size_t{1}; argument_idx < argument_results.size(); ++argument_idx) {
    result_size = _result_size(result_size, argument_results[argument_idx]->size());
  }

  // 3 - Concatenate the values
  std::vector<std::string> result_values(result_size);
  for (const auto& argument_result : argument_results) {
    argument_result->as_view([&](const auto& argument_view) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < result_size; ++chunk_offset) {
        // The actual CONCAT
        result_values[chunk_offset] += argument_view.value(chunk_offset);
      }
    });
  }

  // 4 - Optionally concatenate the nulls (i.e. one argument is null -> result is null) and return
  if (result_is_nullable) {
    std::vector<bool> result_nulls(result_size, false);
    for (const auto& argument_result : argument_results) {
      argument_result->as_view([&](const auto& argument_view) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < result_size; ++chunk_offset) {
          result_nulls[chunk_offset] = result_nulls[chunk_offset] || argument_view.null(chunk_offset);
        }
      });
    }

    return std::make_shared<ExpressionResult<std::string>>(std::move(result_values), std::move(result_nulls));
  } else {
    return std::make_shared<ExpressionResult<std::string>>(std::move(result_values));
  }
}

template <typename R>
std::vector<std::shared_ptr<ExpressionResult<R>>> ExpressionEvaluator::_prune_tables_to_expression_results(
    const std::vector<std::shared_ptr<const Table>>& tables) {

  /**
   * Makes sure each Table in @param tables has only a single column. Materialize this single column into
   * an ExpressionResult and return the vector of resulting ExpressionResults.
   */


  std::vector<std::shared_ptr<ExpressionResult<R>>> results(tables.size());

  for (auto table_idx = size_t{0}; table_idx < tables.size(); ++table_idx) {
    const auto& table = tables[table_idx];

    Assert(table->column_count() == 1, "Expected precisely one column from SubSelect");
    Assert(table->column_data_type(ColumnID{0}) == data_type_from_type<R>(),
           "Expected different DataType from SubSelect");

    std::vector<R> result_values;
    result_values.reserve(table->row_count());

    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& result_column = *table->get_chunk(chunk_id)->get_column(ColumnID{0});
      materialize_values(result_column, result_values);
    }

    if (table->column_is_nullable(ColumnID{0})) {
      std::vector<bool> result_nulls;
      result_nulls.reserve(table->row_count());

      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& result_column = *table->get_chunk(chunk_id)->get_column(ColumnID{0});
        materialize_nulls<R>(result_column, result_nulls);
      }

      results[table_idx] = std::make_shared<ExpressionResult<R>>(std::move(result_values), std::move(result_nulls));
    } else {
      results[table_idx] = std::make_shared<ExpressionResult<R>>(std::move(result_values));
    }
  }

  return results;
}

}  // namespace opossum