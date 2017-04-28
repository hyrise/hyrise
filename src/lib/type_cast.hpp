#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/not_equal.hpp>
#include <boost/hana/size.hpp>
#include <boost/hana/take_while.hpp>
#include <boost/lexical_cast.hpp>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

namespace {

// Returns the index of type T in an Iterable
template <typename Sequence, typename T>
constexpr auto index_of(Sequence const &sequence, T const &element) {
  constexpr auto size = decltype(hana::size(hana::take_while(sequence, hana::not_equal.to(element)))){};
  return decltype(size)::value;
}

// Negates a type trait
template <bool Condition>
struct _neg : public std::true_type {};

template <>
struct _neg<true> : public std::false_type {};

template <typename Condition>
struct neg : public _neg<Condition::value> {};

// Wrapper that makes std::enable_if a bit more readable
template <typename Condition, typename Type = void>
using enable_if = typename std::enable_if<Condition::value, Type>::type;

}  // namespace

// Retrieves the value stored in an AllTypeVariant without conversion
template <typename T>
const T &get(const AllTypeVariant &value) {
  static_assert(hana::contains(types, hana::type_c<T>), "Type not in AllTypeVariant");
  return boost::get<T>(value);
}

// cast methods - from variant to specific type

// Template specialization for everything but integral types
template <typename T>
enable_if<neg<std::is_integral<T>>, T> type_cast(const AllTypeVariant &value) {
  if (value.which() == index_of(types, hana::type_c<T>)) return get<T>(value);

  return boost::lexical_cast<T>(value);
}

// Template specialization for integral types
template <typename T>
enable_if<std::is_integral<T>, T> type_cast(const AllTypeVariant &value) {
  if (value.which() == index_of(types, hana::type_c<T>)) return get<T>(value);

  try {
    return boost::lexical_cast<T>(value);
  } catch (...) {
    return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
  }
}

std::string to_string(const AllTypeVariant &x);

}  // namespace opossum
