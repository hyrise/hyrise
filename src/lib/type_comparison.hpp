#pragma once

#include <boost/lexical_cast.hpp>
#include <functional>
#include <string>
#include <type_traits>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// source: http://stackoverflow.com/questions/16893992/check-if-type-can-be-explicitly-converted
template <class From, class To>
struct is_explicitly_convertible {
  enum { value = std::is_constructible_v<To, From> && !std::is_convertible_v<From, To> };
};

// source: http://stackoverflow.com/questions/27709461/check-if-type-can-be-an-argument-to-boostlexical-caststring
template <typename T, typename = void>
struct is_lex_castable : std::false_type {};

template <typename T>
struct is_lex_castable<T, decltype(void(std::declval<std::ostream&>() << std::declval<T>()))> : std::true_type {};

template <typename T>
inline constexpr bool is_lex_castable_v = is_lex_castable<T>::value;

/* EQUAL */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_equal(L l, R r) {
  return l == r;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_equal(L l,
                                                                                                                R r) {
  return boost::lexical_cast<L>(r) == l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_equal(L l,
                                                                                                                R r) {
  return boost::lexical_cast<R>(l) == r;
}

/* SMALLER */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_smaller(L l, R r) {
  return l < r;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_smaller(L l,
                                                                                                                  R r) {
  return boost::lexical_cast<L>(r) < l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_smaller(L l,
                                                                                                                  R r) {
  return boost::lexical_cast<R>(l) < r;
}

/* GREATER > */
// L and R are implicitly convertible
template <typename L, typename R>
std::enable_if_t<std::is_convertible_v<L, R> && std::is_convertible_v<R, L>, bool> value_greater(L l, R r) {
  return l > r;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_greater(L l,
                                                                                                                  R r) {
  return boost::lexical_cast<L>(r) > l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_greater(L l,
                                                                                                                  R r) {
  return boost::lexical_cast<R>(l) > r;
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

}  // namespace opossum
