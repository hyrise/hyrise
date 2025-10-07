#pragma once

#include <functional>
#include <string>
#include <type_traits>

#include <boost/lexical_cast.hpp>

#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// source: http://stackoverflow.com/questions/16893992/check-if-type-can-be-explicitly-converted
// NOLINTBEGIN(readability-identifier-naming)
template <class From, class To>
struct is_explicitly_convertible {
  enum : uint8_t { value = std::is_constructible_v<To, From> && !std::is_convertible_v<From, To> };
};

// source: http://stackoverflow.com/questions/27709461/check-if-type-can-be-an-argument-to-boostlexical-caststring
template <typename T, typename = void>
struct is_lex_castable : std::false_type {};

template <typename T>
struct is_lex_castable<T, decltype(void(std::declval<std::ostream&>() << std::declval<T>()))> : std::true_type {};

template <typename T>
inline constexpr bool is_lex_castable_v = is_lex_castable<T>::value;

// NOLINTEND(readability-identifier-naming)
/* EQUAL */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_equal(L left, R right) {
  return left == right;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_equal(
    L left, R right) {
  return boost::lexical_cast<L>(right) == left;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_equal(
    L left, R right) {
  return boost::lexical_cast<R>(left) == right;
}

/* SMALLER */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_smaller(const L& left,
                                                                                                 const R& right) {
  return left < right;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_smaller(
    L left, R right) {
  return boost::lexical_cast<L>(right) < left;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_smaller(
    L left, R right) {
  return boost::lexical_cast<R>(left) < right;
}

/* GREATER > */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_greater(const L& left,
                                                                                                 const R& right) {
  return left > right;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_greater(
    L left, R right) {
  return boost::lexical_cast<L>(right) > left;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_greater(
    L left, R right) {
  return boost::lexical_cast<R>(left) > right;
}

// Function that calls a given functor with the correct std comparator. The light version is not instantiated for
// > and >=, reducing the number of instantiated templates by a third.
template <typename Functor>
void with_comparator_light(const PredicateCondition predicate_condition, const Functor& func) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return func(std::equal_to<void>{});

    case PredicateCondition::NotEquals:
      return func(std::not_equal_to<void>{});

    case PredicateCondition::LessThan:
      return func(std::less<void>{});

    case PredicateCondition::LessThanEquals:
      return func(std::less_equal<void>{});

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      Fail("Operator should have been flipped");

    default:
      Fail("Unsupported operator");
  }
}

// Function that calls a functor with a functor that decides whether a value matches a Between-PredicateCondition.
// This function cannot be integrated into with_comparator, because the created function takes 3 instead of 2
// parameters.
template <typename Functor, typename DataType>
void with_between_comparator(const PredicateCondition predicate_condition, const DataType& lower_value,
                             const DataType& upper_value, const Functor& func) {
  // For integral types, we can assume that lower <= upper and therefore save one comparison by rearranging
  // the equation: (x >= a && x < b) === ((x - a) < (b - a)); cf. https://stackoverflow.com/a/17095534/2204581
  // This is quite a bit faster.
  if constexpr (std::is_integral_v<DataType>) {
    const auto lower_bound = is_lower_inclusive_between(predicate_condition) ? lower_value : lower_value + 1;
    const auto upper_bound = is_upper_inclusive_between(predicate_condition) ? upper_value : upper_value - 1;

    DebugAssert(lower_bound <= upper_bound,
                "Query asks for between predicate with empty range. This should have been filtered out earlier.");

    using UnsignedDataType = std::make_unsigned_t<DataType>;
    const auto value_difference = static_cast<UnsignedDataType>(upper_bound - lower_bound);

    return func([lower_bound, value_difference](const DataType value) {
      return static_cast<UnsignedDataType>(value - lower_bound) <= value_difference;
    });
  }

  switch (predicate_condition) {
    case PredicateCondition::BetweenInclusive:
      return func([&lower_value, &upper_value](const DataType& value) {
        return value >= lower_value && value <= upper_value;
      });

    case PredicateCondition::BetweenLowerExclusive:
      return func([&lower_value, &upper_value](const DataType& value) {
        return value > lower_value && value <= upper_value;
      });

    case PredicateCondition::BetweenUpperExclusive:
      return func([&lower_value, &upper_value](const DataType& value) {
        return value >= lower_value && value < upper_value;
      });

    case PredicateCondition::BetweenExclusive:
      return func([&lower_value, &upper_value](const DataType& value) {
        return value > lower_value && value < upper_value;
      });

    default:
      Fail("PredicateCondition is not a Between-PredicateCondition");
  }
}

// Function that calls a given functor with the correct std comparator
template <typename Functor>
void with_comparator(const PredicateCondition predicate_condition, const Functor& func) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return with_comparator_light(predicate_condition, func);

    case PredicateCondition::GreaterThan:
      return func(std::greater<void>{});

    case PredicateCondition::GreaterThanEquals:
      return func(std::greater_equal<void>{});

    default:
      Fail("Unsupported operator");
  }
}

}  // namespace hyrise
