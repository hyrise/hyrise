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
  // Code duplication with lossless_cast.cpp, but it's are already hard enough to read

  const auto source_data_type = data_type_from_all_type_variant(variant);

  // Lossless casting from NULL to NULL is always NULL. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if (source_data_type == DataType::Null && target_data_type == DataType::Null) {
    // We should be able to return {condition, variant} here, but clang-tidy has what I believe to be a false
    // positive. Instead of ignoring it and risking overlooking something, we let the ExpressionEvaluator handle
    // this case. This is only for stupid queries like `WHERE x = NULL`, anyway.
    return std::nullopt;
  }

  // Safe casting between NULL and non-NULL type is not possible. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if ((source_data_type == DataType::Null) != (target_data_type == DataType::Null)) {
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
