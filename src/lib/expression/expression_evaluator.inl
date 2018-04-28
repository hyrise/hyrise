#include "expression_evaluator.hpp"

#include <type_traits>

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "binary_predicate_expression.hpp"
#include "all_parameter_variant.hpp"
#include "arithmetic_expression.hpp"
#include "pqp_column_expression.hpp"
#include "pqp_select_expression.hpp"
#include "value_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/materialize.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"
#include "resolve_type.hpp"

namespace opossum {

struct BothNullableTernaryAnd final {
  void operator()(bool& result_bool,
                  bool& result_null,
                  const bool left_bool,
                  const bool left_null,
                  const bool right_bool,
                  const bool right_null) const {
    result_bool = left_bool && right_bool;
    result_null = (left_null && right_null) || (left_null && right_bool) || (left_bool && right_null);
  }
};

struct LeftNullableTernaryAnd final {
  void operator()(bool& result_bool,
                  bool& result_null,
                  const bool left_bool,
                  const bool left_null,
                  const bool right_bool) const {
    result_bool = left_bool && right_bool;
    result_null = left_null && right_bool;
  }
};

struct BooleanAnd final {
  void operator()(bool& result_bool,
                  const bool left_bool,
                  const bool right_bool) const {
    result_bool = left_bool && right_bool;
  }
};

template<bool null_from_values, bool value_from_null>
struct OperatorTraits {
  static constexpr auto may_produce_null_from_values = null_from_values;
  static constexpr auto may_produce_value_from_null = value_from_null;
  static constexpr auto supported = false;
};

template<typename Functor, typename O, typename L, typename R, bool null_from_values, bool value_from_null>
struct BinaryFunctorWrapper : public OperatorTraits<null_from_values, value_from_null> {
  void operator()(O &result_value,
                  bool& result_null,
                  const L left_value,
                  const bool left_null,
                  const R right_value,
                  const bool right_null) const {
    result_value = Functor{}(left_value, right_value);
    result_null = left_null || right_null;
  }
};

//template<typename T, typename L, typename R> using GreaterThan = BinaryFunctorWrapper<int32_t, std::greater<std::common_type_t<L, R>>, false, false, true>;
//template<typename T, typename L, typename R> using Plus = BinaryFunctorWrapper<T, std::plus<T>, false, false, false>;

//template<typename O, typename L, typename R, typename Enable = void>
//struct GreaterThan {
//  static constexpr auto supported = false;
//};
//template<typename O, typename L, typename R>
//struct GreaterThan<O, L, R, std::enable_if_t<std::is_same_v<int32_t, O> && std::is_same_v<std::string, L> == std::is_same_v<std::string, R>>> {
//  static constexpr auto supported = true;
//  using impl = BinaryFunctorWrapper<std::greater<std::common_type_t<L, R>>, O, L, R, false, false>;
//};

template<template<typename> typename Fn, typename O, typename L, typename R, typename Enable = void>
struct OrderPredicate {
  static constexpr auto supported = false;
};
template<template<typename> typename Fn, typename O, typename L, typename R>
struct OrderPredicate<Fn, O, L, R, std::enable_if_t<std::is_same_v<int32_t, O> && std::is_same_v<std::string, L> == std::is_same_v<std::string, R>>> {
  static constexpr auto supported = true;
  using impl = BinaryFunctorWrapper<Fn<std::common_type_t<L, R>>, O, L, R, false, false>;
};

template<template<typename> typename Fn, typename O, typename L, typename R, bool null_from_values, bool value_from_null, typename Enable = void>
struct ArithmeticFunctor {
  static constexpr auto supported = false;
};
template<template<typename> typename Fn, typename O, typename L, typename R, bool null_from_values, bool value_from_null>
struct ArithmeticFunctor<Fn, O, L, R, null_from_values, value_from_null, std::enable_if_t<!std::is_same_v<std::string, O> && !std::is_same_v<std::string, L> && !std::is_same_v<std::string, R>>> {
  static constexpr auto supported = true;
  using impl = BinaryFunctorWrapper<Fn<O>, O, L, R, false, false>;
};

template<typename O, typename L, typename R> using Equals = OrderPredicate<std::equal_to, O, L, R>;
template<typename O, typename L, typename R> using NotEquals = OrderPredicate<std::not_equal_to, O, L, R>;
template<typename O, typename L, typename R> using GreaterThan = OrderPredicate<std::greater, O, L, R>;
template<typename O, typename L, typename R> using GreaterThanEquals = OrderPredicate<std::greater_equal, O, L, R>;
template<typename O, typename L, typename R> using LessThan = OrderPredicate<std::less, O, L, R>;
template<typename O, typename L, typename R> using LessThanEquals = OrderPredicate<std::less_equal, O, L, R>;

template<typename O, typename L, typename R> using Plus = ArithmeticFunctor<std::plus, O, L, R, true, true>;

template<typename T>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_expression(const AbstractExpression& expression) {
  switch (expression.type) {
    case ExpressionType::Arithmetic:
      return evaluate_arithmetic_expression<T>(static_cast<const ArithmeticExpression&>(expression));

    case ExpressionType::Predicate: {
      const auto& predicate_expression = static_cast<const AbstractPredicateExpression&>(expression);

      if (is_binary_predicate_condition(predicate_expression.predicate_condition)) {
        return evaluate_binary_predicate_expression<T>(static_cast<const BinaryPredicateExpression&>(expression));
      } else {
        Fail("Unsupported Predicate Expression");
      }
    }

    case ExpressionType::Select: {
      const auto* pqp_select_expression = dynamic_cast<const PQPSelectExpression*>(&expression);
      Assert(pqp_select_expression, "Can only evaluate PQPSelectExpression");

      return evaluate_select_expression<T>(*pqp_select_expression);
    }

    case ExpressionType::Column: {
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

    default:
      Fail("ExpressionType evaluation not yet implemented");
  }
}

template<typename T>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_arithmetic_expression(const ArithmeticExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:
      return evaluate_binary_expression<T, Plus>(left, right);

    default:
      Fail("ArithmeticOperator evaluation not yet implemented");
  }
}

template<typename T>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression) {
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

template<typename T, template<typename, typename, typename> typename Functor>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_binary_expression(
const AbstractExpression& left_operand,
const AbstractExpression& right_operand) {
  const auto left_data_type = left_operand.data_type();
  const auto right_data_type = right_operand.data_type();

  ExpressionResult<T> result;

  resolve_data_type(left_data_type, [&](const auto left_data_type_t) {
    using LeftDataType = typename decltype(left_data_type_t)::type;

    const auto left_operands = evaluate_expression<LeftDataType>(left_operand);

    resolve_data_type(right_data_type, [&](const auto right_data_type_t) {
      using RightDataType = typename decltype(right_data_type_t)::type;

      const auto right_operands = evaluate_expression<RightDataType>(right_operand);

      using ConcreteFunctor = Functor<T, LeftDataType, RightDataType>;

      if constexpr (ConcreteFunctor::supported) {
        result = evaluate_binary_operator<T, LeftDataType, RightDataType>(left_operands, right_operands, typename ConcreteFunctor::impl{});
      } else {
        Fail("Operation not supported on strings");
      }
    });
  });

  return result;
}

template<typename ResultDataType,
         typename LeftOperandDataType,
         typename RightOperandDataType,
         typename Functor>
ExpressionEvaluator::ExpressionResult<ResultDataType> ExpressionEvaluator::evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
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

  const auto result_size = _chunk->size();
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
  else if (right_is_value) right_value = boost::get<RightOperandDataType>(right_operands);
  else if (left_is_value && right_is_null) functor(result_value, result_null, left_value, false, right_value, true);
  else if (left_is_null && right_is_value) functor(result_value, result_null, left_value, true, right_value, false);
  else if (left_is_value && right_is_value) functor(result_value, result_null, left_value, false, right_value, false);

  if ((left_is_value || left_is_null) && (right_is_value || right_is_null)) {
    if (result_null) {
      return NullValue{};
    } else {
      return result_value;
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
      functor(result_values[chunk_offset], result_null, (*left_values)[chunk_offset],
              (*left_nulls)[chunk_offset], (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_nullable && right_is_values) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], (*left_nulls)[chunk_offset],
              (*right_values)[chunk_offset], false);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_nullable && right_is_value) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], (*left_nulls)[chunk_offset],
              right_value, false);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_values && right_is_nullable) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(result_values[chunk_offset], result_null,
              (*left_values)[chunk_offset], false,
              (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_values && right_is_values) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], false,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null /* dummy */,
                (*left_values)[chunk_offset], false,
                (*right_values)[chunk_offset], false);
      });
    }
  }
  else if (left_is_values && right_is_value) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                (*left_values)[chunk_offset], false,
                right_value, false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null /* dummy */,
                (*left_values)[chunk_offset], false,
                right_value, false);
      });
    }
  }
  else if (left_is_values && right_is_null) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                left_values[chunk_offset], false,
                right_value, true);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      std::fill(result_nulls.begin(), result_nulls.end(), true);
    }
  }
  else if (left_is_value && right_is_nullable) {
    evaluate_per_row([&](const auto chunk_offset) {
      functor(result_values[chunk_offset], result_null,
              left_value, false,
              (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
      result_nulls[chunk_offset] = result_null;
    });
  }
  else if (left_is_value && right_is_values) {
    if (result_is_nullable) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                left_value, false,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null /* dummy */,
                left_value, false,
                (*right_values)[chunk_offset], false);
      });
    }
  }
  else if (left_is_null && right_is_nullable) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                left_value, true,
                (*right_values)[chunk_offset], (*right_nulls)[chunk_offset]);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      std::fill(result_nulls.begin(), result_nulls.end(), true);
    }
  }
  else if (left_is_null && right_is_values) {
    if constexpr (Functor::may_produce_value_from_null) {
      evaluate_per_row([&](const auto chunk_offset) {
        functor(result_values[chunk_offset], result_null,
                left_value, true,
                (*right_values)[chunk_offset], false);
        result_nulls[chunk_offset] = result_null;
      });
    } else {
      std::fill(result_nulls.begin(), result_nulls.end(), true);
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
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_select_expression(const PQPSelectExpression& expression) {
  for (const auto& parameter : expression.parameters) {
    _ensure_column_materialization(parameter);
  }

  NonNullableValues<T> result;
  result.reserve(_chunk->size());

  std::vector<AllParameterVariant> parameter_values(expression.parameters.size());

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < _chunk->size(); ++chunk_offset) {
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
    Assert(result_table->row_count() == 1, "Expected precisely one row");
    Assert(result_table->column_data_type(ColumnID{0}) == data_type_from_type<T>(), "Expected different DataType");

    const auto& result_column = *result_table->get_chunk(ChunkID{0})->get_column(ColumnID{0});

    std::vector<T> result_value;
    materialize_values(result_column, result_value);

    result.emplace_back(result_value[0]);
  }

  return result;
}
}  // namespace opossum
