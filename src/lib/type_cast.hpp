#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/not_equal.hpp>
#include <boost/hana/size.hpp>
#include <boost/hana/take_while.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <string>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

namespace detail {

// Returns the index of type T in an SegmentIterable
template <typename Sequence, typename T>
constexpr auto index_of(Sequence const& sequence, T const& element) {
  constexpr auto size = decltype(hana::size(hana::take_while(sequence, hana::not_equal.to(element)))){};
  return decltype(size)::value;
}

}  // namespace detail

// Retrieves the value stored in an AllTypeVariant without conversion
template <typename T>
const T& get(const AllTypeVariant& value) {
  static_assert(hana::contains(data_types, hana::type_c<T>), "Type not in AllTypeVariant");
  return boost::get<T>(value);
}

// cast methods - from one type to another

// Simple (i.e., constructible) conversions
template <typename T, typename U,
          typename = std::enable_if_t<std::is_constructible_v<std::decay_t<T>, std::decay_t<U>>>>
inline __attribute__((always_inline)) T type_cast(U&& value) {
  return T{std::forward<T>(value)};
}

// Simple (i.e., copy constructible) conversions
template <typename T, typename U,
          typename = std::enable_if_t<std::is_constructible_v<std::decay_t<T>, std::decay_t<U>>>>
inline __attribute__((always_inline)) T type_cast(const U& value) {
  return value;
}

// convert from string to T
template <typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, std::string>>>
inline __attribute__((always_inline)) T type_cast(const std::string& value) {
  return boost::lexical_cast<T>(value);
}

// convert from T to string
template <typename T, typename U,
          typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string> &&
                                      !std::is_same_v<std::decay_t<U>, std::string>>>
inline __attribute__((always_inline)) std::string type_cast(const U& value) {
  return std::to_string(value);
}

// convert from NullValue to T
template <typename T>
inline __attribute__((always_inline)) T type_cast(const opossum::NullValue&) {
  if constexpr (std::is_same_v<std::decay_t<T>, std::string>) {
    return "NULL";
  } else {
    Fail("Cannot convert from NullValue to anything but string");
  }
}

// If trivial conversion failed, continue here:
template <typename T>
T type_cast_variant(const AllTypeVariant& value) {
  // fast path if the type is the same
  if (value.which() == detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);

  // slow path with conversion
  T converted_value;
  const auto unpack = [&converted_value](const auto& typed_value) { converted_value = type_cast<T>(typed_value); };
  boost::apply_visitor(unpack, value);
  return converted_value;
}

}  // namespace opossum
