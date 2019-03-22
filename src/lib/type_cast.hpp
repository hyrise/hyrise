#pragma once

#include <string>
#include <numeric>

#include "boost/hana/contains.hpp"
#include "boost/hana/integral_constant.hpp"
#include "boost/hana/not_equal.hpp"
#include "boost/hana/size.hpp"
#include "boost/hana/take_while.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/lexical_cast/try_lexical_convert.hpp"
#include "boost/numeric/conversion/cast.hpp"
#include "boost/variant/apply_visitor.hpp"

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
  return static_cast<T>(std::forward<U>(value));
}

// Simple (i.e., copy constructible) conversions
template <typename T, typename U,
          typename = std::enable_if_t<std::is_constructible_v<std::decay_t<T>, std::decay_t<U>>>>
inline __attribute__((always_inline)) T type_cast(const U& value) {
  return static_cast<T>(value);
}

// convert from string to T
template <typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, pmr_string>>>
inline __attribute__((always_inline)) T type_cast(const pmr_string& value) {
  return boost::lexical_cast<T>(value);
}

// convert from T to string
template <typename T, typename U,
          typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, pmr_string> &&
                                      !std::is_same_v<std::decay_t<U>, pmr_string>>>
inline __attribute__((always_inline)) pmr_string type_cast(const U& value) {
  return pmr_string{std::to_string(value)};
}

// convert from NullValue to T
template <typename T>
inline __attribute__((always_inline)) T type_cast(const opossum::NullValue&) {
  if constexpr (std::is_same_v<std::decay_t<T>, pmr_string>) {
    return "NULL";
  } else {
    Fail("Cannot convert from NullValue to anything but string");
  }
}

// If trivial conversion failed, continue here:
template <typename T>
T type_cast_variant(const AllTypeVariant& value) {
  // fast path if the type is the same
  if (value.which() == ::opossum::detail::index_of(data_types_including_null, hana::type_c<T>)) return get<T>(value);

  // slow path with conversion
  T converted_value;
  const auto unpack = [&converted_value](const auto& typed_value) { converted_value = type_cast<T>(typed_value); };
  boost::apply_visitor(unpack, value);
  return converted_value;
}

/**
 *
 * TYPE CAST SAFE
 * TYPE CAST SAFE
 * TYPE CAST SAFE
 * TYPE CAST SAFE
 *
 */

// Identity
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<Target, Source>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return source;
}

// Long to Int
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<int64_t, Source> && std::is_same_v<int32_t, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  if (source < std::numeric_limits<int32_t>::min() || source > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  } else {
    return static_cast<Target>(source);
  }
}

// Int to Long
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<int32_t, Source> && std::is_same_v<int64_t, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return static_cast<Target>(source);
}

// NULL to anything but NULL
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<NullValue, Source> && !std::is_same_v<NullValue, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return std::nullopt;
}

// Anything but NULL to NULL
template <typename Target, typename Source>
std::enable_if_t<!std::is_same_v<NullValue, Source> && std::is_same_v<NullValue, Target>, std::optional<Target>>
type_cast_safe(
    const Source& source) {
  return std::nullopt;
}

// String to Integral
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<pmr_string, Source> && std::is_integral_v<Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  static_assert(std::is_same_v<int32_t, Target> || std::is_same_v<int64_t, Target>, "Expected int32_t or int64_t");

  // We don't want std::stol's exceptions to occur when debugging, thus we're going the c-way: strtol, which
  // communicates conversion errors via errno. See http://c-faq.com/misc/errno.html on why we're setting `errno = 0`
  errno = 0;
  char* end;

  const auto integral = std::strtol(source.c_str(), &end, 10);

  if (errno == 0 && end == source.data() + source.size()) {
    return type_cast_safe<Target>(integral);
  } else {
    return std::nullopt;
  }
}

// String to Floating Point
// NOT SUPPORTED: Some strings (e.g., "5.5") have lossless float representations, others (e.g., "5.3") do not. Allowing
//                String to Float conversion just sets up confusion why one string was convertible and another was not.
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<pmr_string, Source> && std::is_floating_point_v<Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return std::nullopt;
}

// Integral to String
template <typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Source> && std::is_same_v<pmr_string, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return pmr_string{std::to_string(source)};
}

// Floating Point to String
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_same_v<pmr_string, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  // TODO find a lossless float-to-string converter
  return std::nullopt;
}

// Integral to Floating Point
template <typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Source> && std::is_floating_point_v<Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  auto floating_point = static_cast<Target>(source);
  auto integral = static_cast<Source>(floating_point);
  if (source == integral) {
    return floating_point;
  } else {
    return std::nullopt;
  }
}

// Floating Point Type to Integral Type
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_integral_v<Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  auto integral = static_cast<Target>(source);
  auto floating_point = static_cast<Source>(integral);
  if (source == floating_point) {
    return integral;
  } else {
    return std::nullopt;
  }
}

// Floating Point Type to different Floating Point Type
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_floating_point_v<Target> && !std::is_same_v<Source, Target>, std::optional<Target>>
type_cast_safe(const Source& source) {
  auto integral = static_cast<Target>(source);
  auto floating_point = static_cast<Source>(integral);
  if (source == floating_point) {
    return integral;
  } else {
    return std::nullopt;
  }
}

template <typename Target>
std::optional<Target> variant_cast_safe(const AllTypeVariant& variant) {
  std::optional<Target> result;

  const auto source_data_type = data_type_from_all_type_variant(variant);

  // Safe casting from NULL to NULL is always NULL. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if constexpr (std::is_same_v<Target, NullValue>) {
    if (source_data_type == DataType::Null) {
      return NullValue{};
    }
  }

  // Safe casting between NULL and non-NULL type is not possible. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if ((source_data_type == DataType::Null) != std::is_same_v<Target, NullValue>) {
    return std::nullopt;
  }

  resolve_data_type(data_type_from_all_type_variant(variant), [&](auto source_data_type_t) {
    using SourceDataType = typename decltype(source_data_type_t)::type;
    result = type_cast_safe<Target>(boost::get<SourceDataType>(variant));
  });

  return result;
}

std::optional<AllTypeVariant> variant_cast_safe(const AllTypeVariant& variant, DataType target_data_type);

}  // namespace opossum
