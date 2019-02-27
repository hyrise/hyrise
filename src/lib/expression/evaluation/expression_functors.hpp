#pragma once

#include "expression_result.hpp"

/**
 * ExpressionEvaluator internal functor objects.
 */

namespace opossum {

/**
 * Indicates whether T is a valid argument type to a logical expression
 */
template <typename T>
constexpr bool is_logical_operand = std::is_same_v<int32_t, T> || std::is_same_v<NullValue, T>;

// Turn a bool into itself and a NULL into false
bool to_bool(const bool value) { return value; }
bool to_bool(const NullValue& value) { return false; }

// Cast a value/NULL into another type
template <typename T, typename V>
T to_value(const V& v) {
  return static_cast<T>(v);
}
template <typename T>
T to_value(const NullValue& v) {
  return T{};
}

/**
 * SQL's OR which has a ternary NULL logic, e.g., `TRUE OR NULL -> TRUE`
 */
struct TernaryOrEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value = is_logical_operand<Result> && is_logical_operand<ArgA> && is_logical_operand<ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    const auto a_is_true = !a_null && to_bool(a_value);
    const auto b_is_true = !b_null && to_bool(b_value);
    result_value = a_is_true || b_is_true;
    result_null = (a_null || b_null) && !result_value;
  }
};

/**
 * SQL's AND which has a ternary NULL logic, e.g., `FALSE AND NULL -> FALSE`
 */
struct TernaryAndEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value = is_logical_operand<Result> && is_logical_operand<ArgA> && is_logical_operand<ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    // Is this the least verbose way to implement ternary and?
    const auto a_is_true = !a_null && to_bool(a_value);
    const auto b_is_true = !b_null && to_bool(b_value);
    result_value = a_is_true && b_is_true;
    result_null = a_null && b_null;

    if constexpr (!std::is_same_v<NullValue, ArgA>) result_null |= a_value && b_null;
    if constexpr (!std::is_same_v<NullValue, ArgB>) result_null |= b_value && a_null;
  }
};

/**
 * Wrap a STL comparison operator (std::equal_to, ...) so that it exposes a supports::value member and supports
 * NullValues as argument
 */
template <template <typename T> typename Functor>
struct STLComparisonFunctorWrapper {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value =
        std::is_same_v<int32_t, Result> &&
        // LeftIsString -> RightIsNullOrString
        // RightIsString -> LeftIsNullOrString
        (!std::is_same_v<pmr_string, ArgA> || (std::is_same_v<NullValue, ArgB> || std::is_same_v<pmr_string, ArgB>)) &&
        (!std::is_same_v<pmr_string, ArgB> || (std::is_same_v<NullValue, ArgA> || std::is_same_v<pmr_string, ArgA>));
  };

  template <typename Result, typename ArgA, typename ArgB>
  inline static constexpr bool supports_v = supports<Result, ArgA, ArgB>::value;

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result, const ArgA& a, const ArgB& b) {
    if constexpr (std::is_same_v<NullValue, ArgA> || std::is_same_v<NullValue, ArgB>) {
      result = Result{};
    } else {
      result = static_cast<Result>(Functor<std::common_type_t<ArgA, ArgB>>{}(a, b));
    }
  }
};

using EqualsEvaluator = STLComparisonFunctorWrapper<std::equal_to>;
using NotEqualsEvaluator = STLComparisonFunctorWrapper<std::not_equal_to>;
using LessThanEvaluator = STLComparisonFunctorWrapper<std::less>;
using LessThanEqualsEvaluator = STLComparisonFunctorWrapper<std::less_equal>;

/**
 * See STLComparisonFunctorWrappe, but for arithmetic functors (+, -, *)
 * @tparam Functor
 */
template <template <typename T> typename Functor>
struct STLArithmeticFunctorWrapper {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value =
        !std::is_same_v<pmr_string, Result> && !std::is_same_v<pmr_string, ArgA> && !std::is_same_v<pmr_string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result, const ArgA& a, const ArgB& b) {
    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result = Result{};
    } else {
      result = static_cast<Result>(Functor<std::common_type_t<ArgA, ArgB>>{}(a, b));
    }
  }
};

using AdditionEvaluator = STLArithmeticFunctorWrapper<std::plus>;
using SubtractionEvaluator = STLArithmeticFunctorWrapper<std::minus>;
using MultiplicationEvaluator = STLArithmeticFunctorWrapper<std::multiplies>;

// Modulo selects between the operator `%` for integrals and fmod() for floats. Custom NULL logic returns NULL if the
// divisor is NULL
struct ModuloEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value =
        !std::is_same_v<pmr_string, Result> && !std::is_same_v<pmr_string, ArgA> && !std::is_same_v<pmr_string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;
    if (result_null) return;

    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result_value = Result{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        if constexpr (std::is_integral_v<ArgA> && std::is_integral_v<ArgB>) {
          result_value = static_cast<Result>(a_value % b_value);
        } else {
          result_value = static_cast<Result>(fmod(a_value, b_value));
        }
      }
    }
  }
};

// Custom NULL logic returns NULL if the divisor is NULL
struct DivisionEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value =
        !std::is_same_v<pmr_string, Result> && !std::is_same_v<pmr_string, ArgA> && !std::is_same_v<pmr_string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;

    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result_value = Result{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        result_value = static_cast<Result>(a_value / b_value);
      }
    }
  }
};

struct CaseEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct supports {
    static constexpr bool value = (std::is_same_v<pmr_string, ArgA> == std::is_same_v<pmr_string, ArgB>)&&(
        std::is_same_v<pmr_string, ArgA> == std::is_same_v<pmr_string, Result>);
  };

  template <typename Result, typename ArgA, typename ArgB>
  inline static constexpr bool supports_v = supports<Result, ArgA, ArgB>::value;

  // Implementation is in ExpressionEvaluator::_evaluate_case_expression
};

}  // namespace opossum
