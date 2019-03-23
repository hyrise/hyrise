#pragma once

#include <optional>
#include <type_traits>

#include "all_type_variant.hpp"
#include "null_value.hpp"

namespace opossum {

inline std::tuple<bool, int32_t, uint32_t> decompose_floating_point(float f) {
  auto* bits = reinterpret_cast<uint32_t*>(&f);

  auto exponent = static_cast<int32_t>(((*bits) >> 23) & 0xFF);
  auto fraction = (*bits) & 0x7FFFFF;
  auto sign = ((*bits) & 0x8000) != 0;

  return {sign, exponent, fraction};
}

inline std::tuple<bool, int32_t, uint64_t> decompose_floating_point(double f) {
  auto* bits = reinterpret_cast<uint64_t*>(&f);

  auto exponent = static_cast<int32_t>(((*bits) >> 52) & 0x7FF);
  auto fraction = (*bits) & 0xFFFFFFFFFFFFF;
  auto sign = ((*bits) & 0x80000000) != 0;

  return {sign, exponent, fraction};
}

// Identity
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<Target, Source>, std::optional<Target>> lossless_cast(const Source& source) {
  return source;
}

// Long to Int
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<int64_t, Source> && std::is_same_v<int32_t, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  if (source < std::numeric_limits<int32_t>::min() || source > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  } else {
    return static_cast<Target>(source);
  }
}

// Int to Long
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<int32_t, Source> && std::is_same_v<int64_t, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  return static_cast<Target>(source);
}

// NULL to anything but NULL
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<NullValue, Source> && !std::is_same_v<NullValue, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  return std::nullopt;
}

// Anything but NULL to NULL
template <typename Target, typename Source>
std::enable_if_t<!std::is_same_v<NullValue, Source> && std::is_same_v<NullValue, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  return std::nullopt;
}

// String to Integral
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<pmr_string, Source> && std::is_integral_v<Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
  static_assert(std::is_same_v<int32_t, Target> || std::is_same_v<int64_t, Target>, "Expected int32_t or int64_t");

  // We don't want std::stol's exceptions to occur when debugging, thus we're going the c-way: strtol, which
  // communicates conversion errors via errno. See http://c-faq.com/misc/errno.html on why we're setting `errno = 0`
  errno = 0;
  char* end;

  const auto integral = std::strtol(source.c_str(), &end, 10);

  if (errno == 0 && end == source.data() + source.size()) {
    return lossless_cast<Target>(integral);
  } else {
    return std::nullopt;
  }
}

// String to Floating Point
// NOT SUPPORTED: Some strings (e.g., "5.5") have lossless float representations, others (e.g., "5.3") do not. Allowing
//                String to Float conversion just sets up confusion why one string was convertible and another was not.
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<pmr_string, Source> && std::is_floating_point_v<Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  return std::nullopt;
}

// Integral to String
template <typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Source> && std::is_same_v<pmr_string, Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
  return pmr_string{std::to_string(source)};
}

// Floating Point to String
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_same_v<pmr_string, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  // TODO(moritz) find a lossless float-to-string converter
  return std::nullopt;
}

// Integral to Floating Point
template <typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Source> && std::is_floating_point_v<Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
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
std::enable_if_t<std::is_floating_point_v<Source> && std::is_integral_v<Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
  auto [sign, exponent, fraction] = decompose_floating_point(source); // NOLINT

  // Signed zero
  if (exponent == 0 && fraction == 0) {
    return 0;
  }

  // Infinities and NaNs (https://en.wikibooks.org/wiki/Floating_Point/Special_Numbers)
  if (std::is_same_v<float, Source> && exponent == 127) {
    return std::nullopt;
  }
  if (std::is_same_v<double, Source> && exponent == 1023) {
    return std::nullopt;
  }

  auto fraction_mask = std::is_same_v<float, Source> ? 0x7FFFFF : 0xFFFFFFFFFFFFF;
  auto fraction64 = static_cast<uint64_t>(fraction);
  auto adjusted_exponent = exponent - (std::is_same_v<float, Source> ? 127 : 1023);
  auto integer_bit_count = static_cast<int32_t>(sizeof(Target) * CHAR_BIT) - 1;

  if (adjusted_exponent >= 0 && adjusted_exponent < integer_bit_count &&
      ((fraction64 << adjusted_exponent) & fraction_mask) == 0) {
    return static_cast<Target>(source);
  } else {
    return std::nullopt;
  }
}

// Floating Point Type to different Floating Point Type
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_floating_point_v<Target> &&
                     !std::is_same_v<Source, Target>,
                 std::optional<Target>>
lossless_cast(const Source& source) {
  auto integral = static_cast<Target>(source);
  auto floating_point = static_cast<Source>(integral);
  if (source == floating_point) {
    return integral;
  } else {
    return std::nullopt;
  }
}

template <typename Target>
std::optional<Target> lossless_variant_cast(const AllTypeVariant& variant) {
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
    result = lossless_cast<Target>(boost::get<SourceDataType>(variant));
  });

  return result;
}

std::optional<AllTypeVariant> lossless_variant_cast(const AllTypeVariant& variant, DataType target_data_type);

}  // namespace opossum
