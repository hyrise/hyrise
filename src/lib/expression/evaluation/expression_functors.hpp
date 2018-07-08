#pragma once

#include "expression_result.hpp"

namespace opossum {

template <typename T>
constexpr bool is_logical_operand = std::is_same_v<int32_t, T> || std::is_same_v<NullValue, T>;

bool to_bool(const bool value) { return value; }
bool to_bool(const NullValue& value) { return false; }

template <typename T, typename V>
T to_value(const V& v) {
  if constexpr (std::is_same_v<NullValue, V>) {
    return T{};
  } else {
    return v;
  }
}

struct TernaryOr {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value = is_logical_operand<R> && is_logical_operand<A> && is_logical_operand<B>;
  };

  template <typename R, typename A, typename B>
  void operator()(R& result_value, bool& result_null, const A& a_value, const bool a_null, const B& b_value,
                  const bool b_null) {
    const auto a_is_true = !a_null && to_bool(a_value);
    const auto b_is_true = !b_null && to_bool(b_value);
    result_value = a_is_true || b_is_true;
    result_null = (a_null || b_null) && !result_value;
  };
};

struct TernaryAnd {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value = is_logical_operand<R> && is_logical_operand<A> && is_logical_operand<B>;
  };

  template <typename R, typename A, typename B>
  void operator()(R& result_value, bool& result_null, const A& a_value, const bool a_null, const B& b_value,
                  const bool b_null) {
    // Is this the least verbose way to implement ternary and?
    const auto a_is_true = !a_null && to_bool(a_value);
    const auto b_is_true = !b_null && to_bool(b_value);
    result_value = a_is_true && b_is_true;
    result_null = a_null && b_null;

    if constexpr (!std::is_same_v<NullValue, A>) result_null |= a_value && b_null;
    if constexpr (!std::is_same_v<NullValue, B>) result_null |= b_value && a_null;
  };
};

template <template <typename T> typename Functor>
struct STLComparisonFunctorWrapper {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value =
        std::is_same_v<int32_t, R> &&
        // LeftIsString -> RightIsNullOrString
        // RightIsString -> LeftIsNullOrString
        (!std::is_same_v<std::string, A> || (std::is_same_v<NullValue, B> || std::is_same_v<std::string, B>)) &&
        (!std::is_same_v<std::string, B> || (std::is_same_v<NullValue, A> || std::is_same_v<std::string, A>));
  };

  template <typename R, typename A, typename B>
  void operator()(R& result, const A& a, const B& b) {
    if constexpr (std::is_same_v<NullValue, A> || std::is_same_v<NullValue, B>) {
      result = R{};
    } else {
      result = static_cast<R>(Functor<std::common_type_t<A, B>>{}(a, b));
    }
  };
};

using Equals = STLComparisonFunctorWrapper<std::equal_to>;
using NotEquals = STLComparisonFunctorWrapper<std::not_equal_to>;
using GreaterThan = STLComparisonFunctorWrapper<std::greater>;
using GreaterThanEquals = STLComparisonFunctorWrapper<std::greater_equal>;
using LessThan = STLComparisonFunctorWrapper<std::less>;
using LessThanEquals = STLComparisonFunctorWrapper<std::less_equal>;

template <template <typename T> typename Functor>
struct STLArithmeticFunctorWrapper {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value =
        !std::is_same_v<std::string, R> && !std::is_same_v<std::string, A> && !std::is_same_v<std::string, B>;
  };

  template <typename R, typename A, typename B>
  void operator()(R& result, const A& a, const B& b) {
    if constexpr (std::is_same_v<NullValue, R> || std::is_same_v<NullValue, A> || std::is_same_v<NullValue, B>) {
      result = R{};
    } else {
      result = Functor<std::common_type_t<A, B>>{}(a, b);
    }
  };
};

using Addition = STLArithmeticFunctorWrapper<std::plus>;
using Subtraction = STLArithmeticFunctorWrapper<std::minus>;
using Multiplication = STLArithmeticFunctorWrapper<std::multiplies>;

// Modulo selects between the operator `%` for integrals and fmod() for floats. Custom NULL logic returns NULL if the
// divisor is NULL
struct Modulo {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value =
    !std::is_same_v<std::string, R> && !std::is_same_v<std::string, A> && !std::is_same_v<std::string, B>;
  };

  template <typename R, typename A, typename B>
  void operator()(R& result_value, bool& result_null, const A& a_value, const bool a_null, const B& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;
    if (result_null) return;

    if constexpr (std::is_same_v<NullValue, R> || std::is_same_v<NullValue, A> || std::is_same_v<NullValue, B>) {
      result_value = R{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        if constexpr (std::is_integral_v<A> && std::is_integral_v<B>) {
          result_value = a_value % b_value;
        } else {
          result_value = fmod(a_value, b_value);
        }
      }
    }
  };
};

// Custom NULL logic returns NULL if the divisor is NULL
struct Division {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value =
    !std::is_same_v<std::string, R> && !std::is_same_v<std::string, A> && !std::is_same_v<std::string, B>;
  };

  template <typename R, typename A, typename B>
  void operator()(R& result_value, bool& result_null, const A& a_value, const bool a_null, const B& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;

    if constexpr (std::is_same_v<NullValue, R> || std::is_same_v<NullValue, A> || std::is_same_v<NullValue, B>) {
      result_value = R{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        result_value = a_value / b_value;
      }
    }
  };
};

struct Case {
  template <typename R, typename A, typename B>
  struct supports {
    static constexpr bool value = (std::is_same_v<std::string, A> == std::is_same_v<std::string, B>)&&(
        std::is_same_v<std::string, A> == std::is_same_v<std::string, R>);
  };

  // Implementation is in ExpressionEvaluator::_evaluate_case_expression
};

}  // namespace opossum
