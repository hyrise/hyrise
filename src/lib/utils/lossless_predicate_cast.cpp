#include "lossless_predicate_cast.hpp"

#include <cmath>

namespace opossum {

std::optional<float> next_float_towards(const double value, const double towards) {
  // See lossless_cast<float>(double)
  if (value > 340282346638528859811704183484516925440.0 || value < -340282346638528859811704183484516925440.0) {
    return std::nullopt;
  }

  if (value == towards) return std::nullopt;

  const auto casted_value = static_cast<float>(value);

  if (casted_value < value && towards < value) return casted_value;
  if (casted_value > value && towards > value) return casted_value;

  const float next = std::nexttowardf(casted_value, towards);

  // Maybe someone smarter understands all the edge cases of floats. I'd rather be on the safe side.
  if (!std::isfinite(next)) return std::nullopt;

  return next;
}

std::optional<std::pair<PredicateCondition, AllTypeVariant>> lossless_predicate_variant_cast(
    const PredicateCondition condition, const AllTypeVariant& variant, DataType target_data_type) {
  const auto source_data_type = data_type_from_all_type_variant(variant);

  if (source_data_type == DataType::Null || target_data_type == DataType::Null) {
    // We do not deal with NULL values here. Casting from/to NULL has no meaning. While NULL can technically be an
    // argument for a table scan (SELECT ... WHERE a = NULL), it will not produce any results. IS NULL should have
    // been used instead. As such, we do not care about these combinations and let the fallback solution handle it.
    return std::nullopt;
  }

  std::optional<std::pair<PredicateCondition, AllTypeVariant>> result;

  // clang-format off
  boost::apply_visitor([&](const auto& source) {
    resolve_data_type(target_data_type, [&](auto target_data_type_t) {
      using TargetDataType = typename decltype(target_data_type_t)::type;
      result = lossless_predicate_cast<TargetDataType>(condition, source);
    });
  }, variant);
  // clang-format on

  return result;
}

}  // namespace opossum
