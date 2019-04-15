#pragma once

#include <optional>
#include <type_traits>

#include "all_type_variant.hpp"
#include "null_value.hpp"
#include "resolve_type.hpp"

/**
 * This file defines the following casting functions that guarantee **lossless** conversion between data types and
 * return std::nullopt if no lossless conversion was possible.
 *      lossless_cast<Target>(Source)
 *      lossless_variant_cast<Target>(AllTypeVariant)
 *      lossless_variant_cast(Source, DataType)
 *
 * "Lossless" means just that: No information must be lost during casting. Typical lossful operations would be
 *      - float-to-int casts with the float having a non-zero fractional part
 *      - long-to-int casts where the long was > MAX_INT
 *      - string-to-int where the string contained no number, or a number > MAX_INT.
 *      - double-to-float where the significand of the double has bits set that cannot be represented in float
 *
 * Whenever possible you should use these casts over "comfortable" casts such as static_cast or boost::lexical_cast.
 * Those might incur information loss and might thus introduce bugs. See #1306, #1525.
 */

namespace opossum {

// Identity
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<Target, std::decay_t<Source>>, std::optional<Target>> lossless_cast(Source&& source) {
  return std::forward<Source>(source);
}

// Int64 to Int32
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<int64_t, Source> && std::is_same_v<int32_t, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
  if (source < std::numeric_limits<int32_t>::min() || source > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  } else {
    return static_cast<Target>(source);
  }
}

// Int32 to Int64
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
// NOT SUPPORTED: Lossless Float to String conversion is too obscure and rarely needed to justify supporting it.
//                Lossless Float to String conversion might be possible in theory, but standard library functions do not
//                openly support such a conversion
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> && std::is_same_v<pmr_string, Target>, std::optional<Target>>
lossless_cast(const Source& source) {
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

// Floating Point Type to 32/64 bit Integral Type
template <typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Source> &&
                     (std::is_same_v<Target, int32_t> || std::is_same_v<Target, int64_t>),
                 std::optional<Target>>
lossless_cast(const Source& source) {
  static_assert(std::numeric_limits<float>::is_iec559, "IEEE 754 floating point representation expected.");

  auto integral_part = Source{};

  // No lossless float-to-int conversion possible if the source float has a fractional part
  if (std::modf(source, &integral_part) != 0.0) {
    return std::nullopt;
  }

  // Check the floating point value against explicitly defined boundary values. The boundary values are the
  // the closest-to-zero floating point values that are NOT representable in the given integral type.
  // (Note: We evaluated multiple approaches to identify whether a float/double is representable as a int32/64. None
  //        of them was really portable/readable/"clean". The bounding values approach at least makes intuitive sense.)
  if constexpr (std::is_same_v<Source, float> && std::is_same_v<Target, int32_t>) {
    if (source >= 2'147'483'648.0f || source <= -2'147'483'904.0f) return std::nullopt;
  } else if constexpr (std::is_same_v<Source, double> && std::is_same_v<Target, int32_t>) {
    if (source >= 2'147'483'648.0 || source <= -2'147'483'649.0) return std::nullopt;
  } else if constexpr (std::is_same_v<Source, float> && std::is_same_v<Target, int64_t>) {
    if (source >= 9'223'372'036'854'775'808.0f || source <= -9'223'373'136'366'403'584.0f) return std::nullopt;
  } else if constexpr (std::is_same_v<Source, double> && std::is_same_v<Target, int64_t>) {
    if (source >= 9'223'372'036'854'775'808.0 || source <= -9'223'372'036'854'777'856.0) return std::nullopt;
  }

  return static_cast<Target>(source);
}

// float to double
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<float, Source> && std::is_same_v<double, Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
  return static_cast<Target>(source);
}

// double to float
template <typename Target, typename Source>
std::enable_if_t<std::is_same_v<double, Source> && std::is_same_v<float, Target>, std::optional<Target>> lossless_cast(
    const Source& source) {
  static_assert(std::numeric_limits<float>::is_iec559, "IEEE 754 floating point representation expected.");

  // Check the source double against the highest and lowest double that can still be converted to a float
  if (source > 340282346638528859811704183484516925440.0 || source < -340282346638528859811704183484516925440.0) {
    return std::nullopt;
  } else {
    if (static_cast<float>(source) == source) {
      return static_cast<float>(source);
    } else {
      return std::nullopt;
    }
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
