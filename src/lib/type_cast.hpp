#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/not_equal.hpp>
#include <boost/hana/size.hpp>
#include <boost/hana/take_while.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
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

// For convenience, allow `type_cast<T>(T bla);`, but it shouldn't do anything.
// Conversions that don't require lexical_cast handling (e.g., float->double or float->int) are handled here as well.
template <typename T, typename U, typename = std::enable_if_t<std::is_convertible_v<std::decay<U>, std::decay<T>>>>
inline __attribute__((always_inline)) auto&& type_cast(U&& value) {
  return std::forward<T>(value);
}

// If trivial conversion failed, continues here:
// Template specialization for everything but integral types
template <typename T, typename U>
std::enable_if_t<!std::is_integral_v<T>, T> type_cast(const U& value) {
  // For AllTypeVariants, check if it contains the type that we want. In that case, we don't need to convert anything.
  if constexpr (std::is_same_v<U, AllTypeVariant>) {
    if (value.which() == detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);
  }

  return boost::lexical_cast<T>(value);
}

// Template specialization for integral types
template <typename T, typename U>
std::enable_if_t<std::is_integral_v<T>, T> type_cast(const U& value) {
  if constexpr (std::is_same_v<U, AllTypeVariant>) {
    if (value.which() == detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);
  }

  T converted_value;

  if (boost::conversion::try_lexical_convert(value, converted_value)) {
    return converted_value;
  } else {
    return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
  }
}

}  // namespace opossum
