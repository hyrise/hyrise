#pragma once

#include <optional>
#include <string>
#include <type_traits>

#include "boost/variant.hpp"

#include "resolve_type.hpp"

namespace opossum {

template <typename Target>
std::optional<Target> static_variant_cast(const AllTypeVariant& source) {
  if (variant_is_null(source)) return std::nullopt;

  std::optional<Target> result;

  resolve_data_type(data_type_from_all_type_variant(source), [&](const auto source_data_type_t) {
    using SourceDataType = typename decltype(source_data_type_t)::type;

    if constexpr (std::is_same_v<Target, SourceDataType>) {
      result = boost::get<SourceDataType>(source);
    } else {
      if constexpr (std::is_same_v<pmr_string, SourceDataType> == std::is_same_v<pmr_string, Target>) {
        const auto source_value = boost::get<SourceDataType>(source);
        if (source_value > std::numeric_limits<Target>::max()) {
          result = std::numeric_limits<Target>::max();
        } else if (source_value < std::numeric_limits<Target>::lowest()) {
          result = std::numeric_limits<Target>::lowest();
        } else {
          result = static_cast<Target>(boost::get<SourceDataType>(source));
        }
      } else {
        result = boost::lexical_cast<Target>(boost::get<SourceDataType>(source));
      }
    }
  });

  return result;
}

template <typename Target>
Target lenient_variant_cast(const AllTypeVariant& source) {
  const auto target = static_variant_cast<Target>(source);
  Assert(target, "Lenient variant cast failed");
  return *target;
}

}  // namespace opossum
