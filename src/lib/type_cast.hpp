#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/not_equal.hpp>
#include <boost/hana/size.hpp>
#include <boost/hana/take_while.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <boost/lexical_cast.hpp>
#include <string>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

namespace detail {

// Returns the index of type T in an ColumnIterable
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

// cast methods - from variant to specific type

// Template specialization for everything but integral types
template <typename T>
std::enable_if_t<!std::is_integral<T>::value, T> type_cast(const AllTypeVariant& value) {
  if (value.which() == detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);

  return boost::lexical_cast<T>(value);
}

// Template specialization for integral types
template <typename T>
std::enable_if_t<std::is_integral<T>::value, T> type_cast(const AllTypeVariant& value) {
  if (value.which() == detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);

  T converted_value;

  if (boost::conversion::try_lexical_convert(value, converted_value)) {
    return converted_value;
  } else {
    return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
  }
}

}  // namespace opossum
