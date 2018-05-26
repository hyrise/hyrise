#include "expression_evaluator.hpp"

#include <type_traits>

#include "boost/variant/apply_visitor.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
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

using namespace std::string_literals;

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk):
_chunk(chunk)
{
  _output_row_count = _chunk->size();
  _column_materializations.resize(_chunk->column_count());
}


template<bool null_from_values, bool value_from_null, bool per_row_evaluation>
struct OperatorTraits {
  static constexpr auto may_produce_null_from_values = null_from_values;
  static constexpr auto may_produce_value_from_null = value_from_null;
  static constexpr auto force_per_row_evaluation = per_row_evaluation;
};

template<typename Functor, typename O, typename L, typename R, bool null_from_values, bool value_from_null, bool per_row_evaluation>
struct BinaryFunctorWrapper : public OperatorTraits<null_from_values, value_from_null, per_row_evaluation> {
  void operator()(const ChunkOffset chunk_offset, 
                  O &result_value,
                  bool& result_null,
                  const L left_value,
                  const bool left_null,
                  const R right_value,
                  const bool right_null) const {
    result_value = Functor{}(left_value, right_value);
    result_null = left_null || right_null;
  }
};

template<template<typename> typename Fn, typename O, typename L, typename R, typename Enable = void>
struct Comparison {
  static constexpr auto supported = false;
};
template<template<typename> typename Fn, typename O, typename L, typename R>
struct Comparison<Fn, O, L, R, std::enable_if_t<std::is_same_v<int32_t, O> && std::is_same_v<std::string, L> == std::is_same_v<std::string, R>>> :  public BinaryFunctorWrapper<Fn<std::common_type_t<L, R>>, O, L, R, false, false, false> {
  static constexpr auto supported = true;
};

template<typename O, typename L, typename R, typename Enable = void>
struct TernaryOrImpl {
  static constexpr auto supported = false;
};

template<typename O, typename L, typename R>
struct TernaryOrImpl<O, L, R, std::enable_if_t<std::is_same_v<int32_t, O> && std::is_same_v<int32_t, L> && std::is_same_v<int32_t, R>>> : public OperatorTraits<false, true, false> {
  static constexpr auto supported = true;
  void operator()(const ChunkOffset chunk_offset, int32_t &result_value, bool& result_null, const int32_t left_value, const bool left_null,
                  const int32_t right_value, const bool right_null) const {
    const auto left_is_true = !left_null && left_value;
    const auto right_is_true = !right_null && right_value;

    result_value = left_is_true || right_is_true;
    result_null = (left_null || right_null) && !result_value;
  }
};

template<typename O, typename L, typename R, typename Enable = void>
struct CaseNonNullableCondition {
  static constexpr auto supported = false;
};

template<typename O, typename L, typename R>
struct CaseNonNullableCondition<O, L, R, std::enable_if_t<std::is_same_v<std::string, O> == std::is_same_v<std::string, L> && std::is_same_v<std::string, L> == std::is_same_v<std::string, R>>> : public OperatorTraits<false, true, true> {
  static constexpr auto supported = true;

  const std::vector<int32_t> when_values;

  CaseNonNullableCondition(std::vector<int32_t> when_values):
    when_values(when_values) {}
  
  void operator()(const ChunkOffset chunk_offset, O &result_value, bool& result_null, const L left_value, const bool left_null,
                  const R right_value, const bool right_null) const {
    if (when_values[chunk_offset]) {
      if (left_null) {
        result_null = true;
      } else {
        result_null = false;
        result_value = static_cast<O>(left_value);        
      }
    } else {
      if (right_null) {
        result_null = true;
      } else {
        result_null = false;
        result_value = static_cast<O>(right_value);
      }
    }
  }
};

template<typename O, typename L, typename R, typename Enable = void>
struct CaseNullableCondition {
  static constexpr auto supported = false;
};

template<typename O, typename L, typename R>
struct CaseNullableCondition<O, L, R, std::enable_if_t<std::is_same_v<std::string, O> == std::is_same_v<std::string, L> && std::is_same_v<std::string, L> == std::is_same_v<std::string, R>>> : public OperatorTraits<false, true, true> {
  static constexpr auto supported = true;

  const std::vector<int32_t> when_values;
  const std::vector<bool> when_nulls;

  CaseNullableCondition(std::vector<int32_t> when_values, std::vector<bool> when_nulls):
    when_values(when_values), when_nulls(when_nulls) {}

  void operator()(const ChunkOffset chunk_offset, O &result_value, bool& result_null, const L left_value, const bool left_null,
                  const R right_value, const bool right_null) const {
    if (when_nulls[chunk_offset] || when_values[chunk_offset]) {
      if (left_null) {
        result_null = true;
      } else {
        result_null = false;
        result_value = static_cast<O>(left_value);
      }
    } else {
      if (right_null) {
        result_null = true;
      } else {
        result_null = false;
        result_value = static_cast<O>(right_value);
      }
    }
  }
};

template<template<typename> typename Fn, typename O, typename L, typename R, typename Enable = void>
struct Logical {
  static constexpr auto supported = false;
};
template<template<typename> typename Fn, typename O, typename L, typename R>
struct Logical<Fn, O, L, R, std::enable_if_t<std::is_same_v<int32_t, O> && std::is_same_v<int32_t, L> && std::is_same_v<int32_t, R>>> : public BinaryFunctorWrapper<Fn<O>, O, L, R, false, false, false>{
  static constexpr auto supported = true;
};

template<template<typename> typename Fn, typename O, typename L, typename R, typename Enable = void>
struct ArithmeticFunctor {
  static constexpr auto supported = false;
};
template<template<typename> typename Fn, typename O, typename L, typename R>
struct ArithmeticFunctor<Fn, O, L, R, std::enable_if_t<!std::is_same_v<std::string, O> && !std::is_same_v<std::string, L> && !std::is_same_v<std::string, R>>> : public BinaryFunctorWrapper<Fn<O>, O, L, R, false, false, false> {
  static constexpr auto supported = true;
};

// clang-format off
template<typename O, typename L, typename R> using Equals = Comparison<std::equal_to, O, L, R>;
template<typename O, typename L, typename R> using NotEquals = Comparison<std::not_equal_to, O, L, R>;
template<typename O, typename L, typename R> using GreaterThan = Comparison<std::greater, O, L, R>;
template<typename O, typename L, typename R> using GreaterThanEquals = Comparison<std::greater_equal, O, L, R>;
template<typename O, typename L, typename R> using LessThan = Comparison<std::less, O, L, R>;
template<typename O, typename L, typename R> using LessThanEquals = Comparison<std::less_equal, O, L, R>;

template<typename O, typename L, typename R> using And = Logical<std::logical_and, O, L, R>;
// template<typename O, typename L, typename R> using TernaryOr = TernaryOrImpl<O, L, R>;

template<typename O, typename L, typename R> using Addition = ArithmeticFunctor<std::plus, O, L, R>;
template<typename O, typename L, typename R> using Subtraction = ArithmeticFunctor<std::minus, O, L, R>;
template<typename O, typename L, typename R> using Multiplication = ArithmeticFunctor<std::multiplies, O, L, R>;
template<typename O, typename L, typename R> using Division = ArithmeticFunctor<std::divides, O, L, R>;
// clang-format on


// clang-format off
struct TernaryOr {
  template<typename R, typename A, typename B> struct supports           { static constexpr bool value = std::is_same_v<int32_t, R> && std::is_same_v<int32_t, A> && std::is_same_v<int32_t, B>; };
  template<typename A, typename B>             struct evaluate_as_series { static constexpr bool value = is_series<A>::value || is_series<B>::value; };
  static constexpr auto may_produce_value_from_null = true;

  template<typename R, typename A, typename B>
  static void perform(const ChunkOffset chunk_offset, R& result, const A& a, const B& b) {
    const auto left_is_true = !get_null(a, chunk_offset) && static_cast<bool>(get_value(a, chunk_offset));
    const auto right_is_true = !get_null(b, chunk_offset) && static_cast<bool>(get_value(b, chunk_offset));
    const auto result_is_true = left_is_true || right_is_true;

    set_value(result, chunk_offset, static_cast<typename expression_result_data_type<R>::type>(result_is_true));
    set_null(result, chunk_offset, (get_null(a, chunk_offset) || get_null(b, chunk_offset)) && !result_is_true);
  };
};
// clang-format on

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
        return evaluate_in_expression<T>(static_cast<const InExpression&>(expression));
      } else {
        Fail("Unsupported Predicate Expression");
      }
    }

    case ExpressionType::Select: {
      const auto* pqp_select_expression = dynamic_cast<const PQPSelectExpression*>(&expression);
      Assert(pqp_select_expression, "Can only evaluate PQPSelectExpression");

      return evaluate_select_expression_for_chunk<T>(*pqp_select_expression);
    }

    case ExpressionType::Column: {
      Assert(_chunk, "Cannot access Columns in this Expression as it doesn't operate on a Table/Chunk");

      const auto* pqp_column_expression = dynamic_cast<const PQPColumnExpression*>(&expression);
      Assert(pqp_column_expression, "Can only evaluate PQPColumnExpressions");

      const auto& column = *_chunk->get_column(pqp_column_expression->column_id);

      std::vector<T> values;
      materialize_values(column, values);

      if (pqp_column_expression->is_nullable()) {
        std::vector<bool> nulls;
        materialize_nulls<T>(column, nulls);

        return std::make_pair(values, nulls);
      }

      return values;
    }

    case ExpressionType::Value: {
      const auto& value_expression = static_cast<const ValueExpression &>(expression);
      const auto& value = value_expression.value;

      Assert(value.type() == typeid(T), "Can't evaluate ValueExpression to requested type T");

      return boost::get<T>(value);
    }

    case ExpressionType::Function:
      return evaluate_function_expression<T>(static_cast<const FunctionExpression&>(expression));

    case ExpressionType::Case:
      return evaluate_case_expression<T>(static_cast<const CaseExpression&>(expression));

    case ExpressionType::Exists:
      return evaluate_exists_expression<T>(static_cast<const ExistsExpression&>(expression));

    case ExpressionType::Extract:
      return evaluate_extract_expression<T>(static_cast<const ExtractExpression&>(expression));

    case ExpressionType::Not:
      Fail("Not not yet implemented");

    case ExpressionType::Array:
      Fail("Can't 'evaluate' an Array as a top level expression, can only handle them as part of an InExpression. "
           "The expression you are passing in is probably malformed");

    case ExpressionType::External:
    case ExpressionType::ValuePlaceholder:
    case ExpressionType::Mock:
      Fail("Can't handle External/ValuePlaceholders/Mocks since they don't have a value.");

    case ExpressionType::Aggregate:
      Fail("ExpressionEvaluator doesn't support Aggregates, use the Aggregate Operator to compute them");
  }
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_function_expression(const FunctionExpression& expression) {
  ExpressionResult<T> result;

  switch (expression.function_type) {
    case FunctionType::Substring:
      // clang-format off
      if constexpr (std::is_same_v<T, std::string>) {
        resolve_data_type(expression.arguments[1]->data_type(), [&](const auto offset_data_type_t) {
          using OffsetDataType = typename decltype(offset_data_type_t)::type;

          resolve_data_type(expression.arguments[2]->data_type(), [&](const auto char_count_data_type_t) {
            using CharCountDataType = typename decltype(char_count_data_type_t)::type;

            if constexpr(std::is_integral_v<OffsetDataType> && std::is_integral_v<CharCountDataType>) {
              result = evaluate_substring(
                evaluate_expression<std::string>(*expression.arguments[0]),
                evaluate_expression<OffsetDataType>(*expression.arguments[1]),
                evaluate_expression<OffsetDataType>(*expression.arguments[2]));
            } else {
              Fail("SUBSTRING parameters 2 and 3 need to be integral");
            }
          });
        });
      } else {
        Fail("SUBSTRING can only return String");
      }
      // clang-format on
  }

  return result;
}

template<typename OffsetDataType, typename CharCountDataType>
ExpressionResult<std::string> ExpressionEvaluator::evaluate_substring(const ExpressionResult<std::string>& string_result,
                                                 const ExpressionResult<OffsetDataType>& offset_result,
                                                 const ExpressionResult<CharCountDataType>& char_count_result) {
  Fail("Not yet implemented");
//  /**
//   *
//   */
//
//  auto result_values = std::vector<std::string>(_output_row_count);
//  auto result_nulls = std::vector<bool>(_output_row_count);
//
//  for (auto chunk_offset = 0; chunk_offset < _output_row_count; ++chunk_offset) {
//
//  }
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_arithmetic_expression(const ArithmeticExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:       return evaluate_binary_expression<T, Addition>(left, right);
    case ArithmeticOperator::Subtraction:    return evaluate_binary_expression<T, Subtraction>(left, right);
    case ArithmeticOperator::Multiplication: return evaluate_binary_expression<T, Multiplication>(left, right);
    case ArithmeticOperator::Division:       return evaluate_binary_expression<T, Division>(left, right);

    default:
      Fail("ArithmeticOperator evaluation not yet implemented");
  }
  // clang-format on
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.predicate_condition) {
    case PredicateCondition::Equals:            return evaluate_binary_expression<T, Equals>(left, right);
    case PredicateCondition::NotEquals:         return evaluate_binary_expression<T, NotEquals>(left, right);
    case PredicateCondition::LessThan:          return evaluate_binary_expression<T, LessThan>(left, right);
    case PredicateCondition::LessThanEquals:    return evaluate_binary_expression<T, LessThanEquals>(left, right);
    case PredicateCondition::GreaterThan:       return evaluate_binary_expression<T, GreaterThan>(left, right);
    case PredicateCondition::GreaterThanEquals: return evaluate_binary_expression<T, GreaterThanEquals>(left, right);

    default:
      Fail("PredicateCondition evaluation not yet implemented");
  }
  // clang-format on
}


template<typename T, template<typename...> typename Functor>
ExpressionResult<T> ExpressionEvaluator::evaluate_binary_expression(
const AbstractExpression& left_operand,
const AbstractExpression& right_operand) {
  const auto left_is_null = left_operand.data_type() == DataType::Null;
  const auto right_is_null = right_operand.data_type() == DataType::Null;

  if (left_is_null && right_is_null) return NullValue{};

  ExpressionResult<T> result;

  if (left_is_null) {
    resolve_data_type(right_operand.data_type(), [&](const auto right_data_type_t) {
      using RightDataType = typename decltype(right_data_type_t)::type;
      const auto right_result = evaluate_expression<RightDataType>(right_operand);
      using ConcreteFunctor = Functor<T, T, RightDataType>;

      if constexpr (ConcreteFunctor::supported) {
        result = evaluate_binary_operator<T, T, RightDataType>(ExpressionResult<T>(NullValue{}), right_result, ConcreteFunctor{});
      } else {
        Fail("Operation not supported on the given types");
      }
    });
  } else if (right_is_null) {
    resolve_data_type(left_operand.data_type(), [&](const auto left_data_type_t) {
      using LeftDataType = typename decltype(left_data_type_t)::type;
      const auto left_result = evaluate_expression<LeftDataType>(left_operand);
      using ConcreteFunctor = Functor<T, LeftDataType, T>;

      if constexpr (ConcreteFunctor::supported) {
        result = evaluate_binary_operator<T, LeftDataType, T>(left_result, ExpressionResult<T>(NullValue{}), ConcreteFunctor{});
      } else {
        Fail("Operation not supported on the given types");
      }
    });

  } else {
    resolve_data_type(left_operand.data_type(), [&](const auto left_data_type_t) {
      using LeftDataType = typename decltype(left_data_type_t)::type;

      const auto left_operands = evaluate_expression<LeftDataType>(left_operand);

      resolve_data_type(right_operand.data_type(), [&](const auto right_data_type_t) {
        using RightDataType = typename decltype(right_data_type_t)::type;

        const auto right_operands = evaluate_expression<RightDataType>(right_operand);

        using ConcreteFunctor = Functor<T, LeftDataType, RightDataType>;

        if constexpr (ConcreteFunctor::supported) {
          result = evaluate_binary_operator<T, LeftDataType, RightDataType>(left_operands, right_operands, ConcreteFunctor{});
        } else {
          Fail("Operation not supported on the given types");
        }
      });
    });
  }

  return result;
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_logical_expression(const LogicalExpression& expression) {
  Fail("LogicalExpression can only output int32_t");
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_in_expression(const InExpression& expression) {
  Fail("InExpression supports only int32_t as result");
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_case_expression(const CaseExpression& case_expression) {
  const auto when = evaluate_expression<int32_t>(*case_expression.when());

  /**
   * Handle cases where the CASE condition ("WHEN") is a fixed value/NULL (e.g. CASE 5+3 > 2 THEN ... ELSE ...)
   *    fixed_branch: std::nullopt, if CASE condition is not a value/NULL
   *    fixed_branch: CaseBranch::Then, if CASE condition if value is not FALSE
   *    fixed_branch: CaseBranch::Else, if CASE condition if value false or NULL
   * This avoids computing branches we don't need to compute.
   */
  enum class CaseBranch { Then, Else };
  auto fixed_branch = std::optional<CaseBranch>{};

  if (is_value(when)) fixed_branch = boost::get<int32_t>(when) != 0 ? CaseBranch::Then : CaseBranch::Else;
  if (is_null(when)) fixed_branch = CaseBranch::Else;

  if (fixed_branch) {
    switch (*fixed_branch) {
      case CaseBranch::Then: return evaluate_expression<T>(*case_expression.then());
      case CaseBranch::Else: return evaluate_expression<T>(*case_expression.else_());
    }
  }
  
  /**
   * Handle cases where the CASE condition is a per-row expression
   */
  const auto then_is_null = case_expression.then()->data_type() == DataType::Null;
  const auto else_is_null = case_expression.else_()->data_type() == DataType::Null;

  if (then_is_null && else_is_null) return NullValue{};

  ExpressionResult<T> result;

  if (then_is_null) {
    resolve_data_type(case_expression.else_()->data_type(), [&](const auto else_data_type_t) {
      using ElseDataType = typename decltype(else_data_type_t)::type;
      const auto else_result = evaluate_expression<ElseDataType>(*case_expression.then());

      result = evaluate_case<T>(when, ExpressionResult<T>(NullValue{}), else_result);
    });

  } else if (else_is_null) {
    resolve_data_type(case_expression.then()->data_type(), [&](const auto then_data_type_t) {
      using ThenDataType = typename decltype(then_data_type_t)::type;
      const auto then_result = evaluate_expression<ThenDataType>(*case_expression.then());

      result = evaluate_case<T>(when, then_result, ExpressionResult<T>(NullValue{}));
    });

  } else {
    resolve_data_type(case_expression.then()->data_type(), [&](const auto then_data_type_t) {
      using ThenDataType = typename decltype(then_data_type_t)::type;
      const auto then_result = evaluate_expression<ThenDataType>(*case_expression.then());

      resolve_data_type(case_expression.else_()->data_type(), [&](const auto else_data_type_t) {
        using ElseDataType = typename decltype(else_data_type_t)::type;
        const auto else_result = evaluate_expression<ElseDataType>(*case_expression.else_());

        result = evaluate_case<T>(when, then_result, else_result);
      });
    });
  }

  return result;
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_exists_expression(const ExistsExpression& exists_expression) {
  std::vector<T> result_values(_output_row_count);

  const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(exists_expression.select());
  for (const auto& parameter : pqp_select_expression->parameters) {
    _ensure_column_materialization(parameter);
  }

  resolve_data_type(pqp_select_expression->data_type(), [&](const auto select_data_type_t) {
    using SelectDataType = typename decltype(select_data_type_t)::type;

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
      const auto select_result = evaluate_select_expression_for_row<SelectDataType>(*pqp_select_expression, chunk_offset);
      const auto& select_result_values = boost::get<NonNullableValues<SelectDataType>>(select_result);
      result_values[chunk_offset] = !select_result_values.empty();
    }
  });

  return result_values;
}

template<>
ExpressionResult<std::string> ExpressionEvaluator::evaluate_extract_expression<std::string>(const ExtractExpression& extract_expression) {
  const auto from_result = evaluate_expression<std::string>(*extract_expression.from());

  switch (extract_expression.datetime_component) {
    case DatetimeComponent::Year: return evaluate_extract_substr<0, 4>(from_result);
    case DatetimeComponent::Month: return evaluate_extract_substr<5, 2>(from_result);
    case DatetimeComponent::Day: return evaluate_extract_substr<8, 2>(from_result);

    case DatetimeComponent::Hour:
    case DatetimeComponent::Minute:
    case DatetimeComponent::Second:
      Fail("Hour, Minute and Second not available in String Datetimes");
  }
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_extract_expression(const ExtractExpression& extract_expression) {
  Fail("Only Strings (YYYY-MM-DD) supported for Dates right now");
}

template<size_t offset, size_t count>
ExpressionResult<std::string> ExpressionEvaluator::evaluate_extract_substr(const ExpressionResult<std::string>& from_result) {
  if (is_null(from_result)) {
    return NullValue{};

  } else if (is_value(from_result)) {
    const auto& date = boost::get<std::string>(from_result);
    DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
    return date.substr(offset, count);

  } else if (is_non_nullable_values(from_result)) {
    const auto& from_values = boost::get<NonNullableValues<std::string>>(from_result);

    auto result_values = NonNullableValues<std::string>(_output_row_count);

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
      const auto& date = from_values[chunk_offset];
      DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
      result_values[chunk_offset] = date.substr(offset, count);
    }

    return result_values;

  } else if (is_nullable_values(from_result)) {
    const auto& from_values_and_nulls = boost::get<NullableValues<std::string>>(from_result);
    const auto& from_values = from_values_and_nulls.first;
    const auto& from_nulls = from_values_and_nulls.second;

    auto result_values = std::vector<std::string>(_output_row_count);

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
      const auto& date = from_values[chunk_offset];
      DebugAssert(date.size() == 10, "String Date format required to be strictly YYYY-MM-DD");
      result_values[chunk_offset] = date.substr(offset, count);
    }

    return std::make_pair(result_values, from_nulls);

  } else {
    Fail("Can't EXTRACT from this Expression");
  }
}

template<typename ResultDataType,
typename ThenDataType,
typename ElseDataType>
ExpressionResult<ResultDataType> ExpressionEvaluator::evaluate_case(const ExpressionResult<int32_t>& when_result,
                                               const ExpressionResult<ThenDataType>& then_result,
                                               const ExpressionResult<ElseDataType>& else_result) {
  if (is_nullable_values(when_result)) {
    using CaseFunctor = CaseNullableCondition<ResultDataType, ThenDataType, ElseDataType>;

    if constexpr (CaseFunctor::supported) {
      const auto& when_values = boost::get<NullableValues<int32_t>>(when_result).first;
      const auto& when_nulls = boost::get<NullableValues<int32_t>>(when_result).second;

      return evaluate_binary_operator<ResultDataType>(then_result, else_result, CaseFunctor(when_values, when_nulls));
    } else {
      Fail("Operand types not support for CASE");
    }
  } else {
    using CaseFunctor = CaseNonNullableCondition<ResultDataType, ThenDataType, ElseDataType>;

    if constexpr (CaseFunctor::supported) {
      const auto& when_values = boost::get<NonNullableValues<int32_t>>(when_result);

      return evaluate_binary_operator<ResultDataType>(then_result, else_result, CaseFunctor{when_values});
    } else {
      Fail("Operand types not support for CASE");
    }
  }
}

template<typename ResultDataType,
typename LeftOperandDataType,
typename RightOperandDataType,
typename Functor>
ExpressionResult<ResultDataType> ExpressionEvaluator::evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
                                                                                                    const ExpressionResult<RightOperandDataType>& right_operands,
                                                                                                    const Functor &functor) {

  const auto left_is_nullable = left_operands.type() == typeid(NullableValues<LeftOperandDataType>);
  const auto left_is_values = left_operands.type() == typeid(NonNullableValues<LeftOperandDataType>);
  const auto left_is_value = left_operands.type() == typeid(LeftOperandDataType);
  const auto left_is_null = left_operands.type() == typeid(NullValue);
  const auto right_is_nullable = right_operands.type() == typeid(NullableValues<RightOperandDataType>);
  const auto right_is_values = right_operands.type() == typeid(NonNullableValues<RightOperandDataType>);
  const auto right_is_value = right_operands.type() == typeid(RightOperandDataType);
  const auto right_is_null = right_operands.type() == typeid(NullValue);

  const auto result_size = _output_row_count;
  std::vector<ResultDataType> result_values(result_size);
  std::vector<bool> result_nulls;

  // clang-format off
  if (left_is_null && right_is_null) return NullValue{};

  auto result_value = ResultDataType{};
  auto result_null = false;
  auto left_value = LeftOperandDataType{};
  auto right_value = RightOperandDataType{};

  /**
   * Compute single value/null cases
   */
  if (left_is_value) left_value = boost::get<LeftOperandDataType>(left_operands);
  if (right_is_value) right_value = boost::get<RightOperandDataType>(right_operands);

  if (!Functor::force_per_row_evaluation) {
    if (left_is_value && right_is_null) functor(0, result_value, result_null, left_value, false, right_value, true);
    else if (left_is_null && right_is_value)
      functor(0, result_value, result_null, left_value, true, right_value, false);
    else if (left_is_value && right_is_value)
      functor(0, result_value, result_null, left_value, false, right_value, false);

    if ((left_is_value || left_is_null) && (right_is_value || right_is_null)) {
      if (result_null) {
        return NullValue{};
      } else {
        return result_value;
      }
    }
  }

  /**
   * Per-row cases
   */
  const std::vector<LeftOperandDataType>* left_values = nullptr;
  const std::vector<bool>* left_nulls = nullptr;
  const std::vector<RightOperandDataType>* right_values = nullptr;
  const std::vector<bool>* right_nulls = nullptr;

  if (left_is_nullable) {
    const auto& values_and_nulls = boost::get<NullableValues<LeftOperandDataType>>(left_operands);
    left_values = &values_and_nulls.first;
    left_nulls = &values_and_nulls.second;
  }
  if (left_is_values) left_values = &boost::get<NonNullableValues<LeftOperandDataType>>(left_operands);

  if (right_is_nullable) {
    const auto& values_and_nulls = boost::get<NullableValues<RightOperandDataType>>(right_operands);
    right_values = &values_and_nulls.first;
    right_nulls = &values_and_nulls.second;
  }
  if (right_is_values) right_values = &boost::get<NonNullableValues<RightOperandDataType>>(right_operands);

  const auto result_is_nullable = left_is_nullable || left_is_null || right_is_nullable || right_is_null || Functor::may_produce_null_from_values;

  if (result_is_nullable) result_nulls.resize(result_size);

  /**
   *
   */
  const auto evaluate_per_row = [&](const auto& fn) {
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < result_size; ++chunk_offset) {
      fn(chunk_offset);
    }
  };

  if (left_is_nullable && right_is_nullable) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null, (*left_values)[chunk_offset],
              (*left_nulls)[chunk_offset], (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_nullable && right_is_values) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], (*left_nulls)[chunk_offset],
              (*right_values)[chunk_offset], false);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_nullable && right_is_value) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], (*left_nulls)[chunk_offset],
              right_value, false);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_nullable && right_is_null) {
    if (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], (*left_nulls)[chunk_offset],
                RightOperandDataType{}, true);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else if (left_is_values && right_is_nullable) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], false,
              (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_values && right_is_values) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], false,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null /* dummy */,
                (*left_values)[chunk_offset], false,
                (*right_values)[chunk_offset], false);
      });
    }
  }
  else if (left_is_values && right_is_value) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], false,
                right_value, false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null /* dummy */,
                (*left_values)[chunk_offset], false,
                right_value, false);
      });
    }
  }
  else if (left_is_values && right_is_null) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], false,
                right_value, true);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else if (left_is_value && right_is_nullable) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null,
              left_value, false,
              (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_value && right_is_values) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, false,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null /* dummy */,
                left_value, false,
                (*right_values)[chunk_offset], false);
      });
    }
  }
  else if (left_is_value && right_is_value) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, false,
                right_value, false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null /* dummy */,
                left_value, false,
                right_value, false);
      });
    }
  }
  else if (left_is_value && right_is_null) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(chunk_offset, result_values[chunk_offset], result_null,
              left_value, false,
              right_value /* dummy */, true);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_null && right_is_nullable) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, true,
                (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else if (left_is_null && right_is_values) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, true,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else if (left_is_null && right_is_value) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, true,
                right_value, false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else if (left_is_null && right_is_null) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(chunk_offset, result_values[chunk_offset], result_null,
                left_value, true,
                right_value, true);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      return NullValue{};
    }
  }
  else {
    Fail("Operand types not implemented");
  }

  if (result_is_nullable) {
    return std::make_pair(result_values, result_nulls);
  } else {
    return result_values;
  }
  // clang-format on

  return result_values;
}


template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_select_expression_for_chunk(
const PQPSelectExpression &expression) {
  for (const auto& parameter : expression.parameters) {
    _ensure_column_materialization(parameter);
  }

  NonNullableValues<T> result;
  result.reserve(_output_row_count);

  std::vector<AllParameterVariant> parameter_values(expression.parameters.size());

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
    const auto select_result = evaluate_select_expression_for_row<T>(expression, chunk_offset);
    const auto& select_result_values = boost::get<NonNullableValues<T>>(select_result);

    Assert(select_result_values.size() == 1, "Expected precisely one row");
    result.emplace_back(select_result_values[0]);
  }

  return result;
}

template<typename T>
ExpressionResult<T> ExpressionEvaluator::evaluate_select_expression_for_row(const PQPSelectExpression& expression, const ChunkOffset chunk_offset) {
  Assert(expression.parameters.empty() || _chunk, "Sub-SELECT references external Columns but Expression doesn't operate on a Table/Chunk");

  std::vector<AllParameterVariant> parameter_values(expression.parameters.size());

  for (auto parameter_idx = size_t{0}; parameter_idx < expression.parameters.size(); ++parameter_idx) {
    const auto parameter_column_id = expression.parameters[parameter_idx];
    const auto& column = *_chunk->get_column(parameter_column_id);

    resolve_data_type(column.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      parameter_values[parameter_idx] = AllTypeVariant{static_cast<ColumnMaterialization<ColumnDataType>&>(*_column_materializations[parameter_column_id]).values[chunk_offset]};
    });
  }

  auto row_pqp = expression.pqp->recreate(parameter_values);

  SQLQueryPlan query_plan;
  query_plan.add_tree_by_root(row_pqp);
  const auto tasks = query_plan.create_tasks();
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  const auto result_table = row_pqp->get_output();

  Assert(result_table->column_count() == 1, "Expected precisely one column");
  Assert(result_table->column_data_type(ColumnID{0}) == data_type_from_type<T>(), "Expected different DataType");

  std::vector<T> result_values;
  result_values.reserve(result_table->row_count());

  for (auto chunk_id = ChunkID{0}; chunk_id < result_table->chunk_count(); ++chunk_id) {
    const auto &result_column = *result_table->get_chunk(chunk_id)->get_column(ColumnID{0});
    materialize_values(result_column, result_values);
  }

  return result_values;
}

void ExpressionEvaluator::_ensure_column_materialization(const ColumnID column_id) {
  Assert(_chunk, "Expression doesn't operate on a Chunk");
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

    if (result.type() == typeid(NonNullableValues<ColumnDataType>)) {
      const auto& result_values = boost::get<NonNullableValues<ColumnDataType>>(result);
      values = pmr_concurrent_vector<ColumnDataType>(result_values.begin(), result_values.end());

    } else if (result.type() == typeid(NullableValues<ColumnDataType>)) {
      const auto& result_values_and_nulls = boost::get<NullableValues<ColumnDataType>>(result);
      const auto& result_values = result_values_and_nulls.first;
      const auto& result_nulls = result_values_and_nulls.second;
      has_nulls = true;

      values = pmr_concurrent_vector<ColumnDataType>(result_values.begin(), result_values.end());
      nulls = pmr_concurrent_vector<bool>(result_nulls.begin(), result_nulls.end());

    } else if (result.type() == typeid(NullValue)) {
      values.resize(_output_row_count, ColumnDataType{});
      nulls.resize(_output_row_count, true);
      has_nulls = true;

    } else if (result.type() == typeid(ColumnDataType)) {
      values.resize(_output_row_count, boost::get<ColumnDataType>(result));

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
ExpressionResult<int32_t> ExpressionEvaluator::evaluate_logical_expression<int32_t>(const LogicalExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.logical_operator) {
    case LogicalOperator::Or:  return evaluate_binary<int32_t, TernaryOr>(left, right);
    case LogicalOperator::And: return evaluate_binary_expression<int32_t, And>(left, right);
  }
  // clang-format on
}

template<>
ExpressionResult<int32_t> ExpressionEvaluator::evaluate_in_expression<int32_t>(const InExpression& in_expression) {
  const auto& left_expression = *in_expression.value();
  const auto& right_expression = *in_expression.set();

  std::vector<int32_t> result_values;
  std::vector<bool> result_nulls;

  if (right_expression.type == ExpressionType::Array) {
    const auto& array_expression = static_cast<const ArrayExpression&>(right_expression);

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











template<typename R, typename Functor>
ExpressionResult<R> ExpressionEvaluator::evaluate_binary(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression) {
  ExpressionResult<R> result_variant;

  resolve_expression(left_expression, [&](const auto& left_result) {
    using LeftResultType = std::decay_t<decltype(left_result)>;
    using LeftDataType = typename expression_result_data_type<LeftResultType>::type;

    resolve_expression(right_expression, [&](const auto& right_result) {
      using RightResultType = std::decay_t<decltype(right_result)>;
      using RightDataType = typename expression_result_data_type<RightResultType>::type;

      if constexpr (Functor::template supports<R, LeftDataType, RightDataType>::value) {
        using ResultType = typename resolve_result_type<R, LeftResultType, RightResultType, Functor>::type;

        ResultType result;

        if constexpr (is_series<ResultType>::value) {
          expression_result_allocate(result, _output_row_count);

          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _output_row_count; ++chunk_offset) {
            Functor::perform(chunk_offset, result, left_result, right_result);
          }
        } else {
          Functor::perform(ChunkOffset{0}, result, left_result, right_result);
        }

        result_variant = result;

      } else {
        Fail("Operation not supported: "s + typeid(R).name() + " - " + typeid(LeftResultType).name() + " - " + typeid(RightResultType).name());
      }
    });
  });

  return result_variant;
}

template<typename Functor>
void ExpressionEvaluator::resolve_expression(const AbstractExpression& expression, const Functor& fn) {
  // resolve_data_type() doesn't support Null, so we have handle it explicitly
  if (expression.data_type() == DataType::Null) {
    fn(NullValue{});
  } else {
    resolve_data_type(expression.data_type(), [&] (const auto data_type_t) {
      using ExpressionDataType = typename decltype(data_type_t)::type;
      const auto expression_result_variant = evaluate_expression<ExpressionDataType>(expression);

      boost::apply_visitor([&](const auto& expression_result) {
        fn(expression_result);
      }, expression_result_variant);
    });
  }
}

}  // namespace opossum