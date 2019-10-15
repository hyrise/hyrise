#pragma once

#include "lossless_cast.hpp"
#include "types.hpp"

namespace opossum {
std::optional<float> next_float_towards(double value, double towards);

// Even if lossless_cast is not successful, we might be able to do a cast if we know the predicate that is used.
// As 3.1 has different float representations as a double and as a float (and neither is exact), lossless_cast will
// fail. However, `float(a) > double(3.1)` can be reformulated as `float(a) >= float(3.1000001430511474609375)`.
// In the table scan, this helps us harmonizing the input types and using more efficient scan implementations without
// falling back to more general implementations.
//
// TODO(anyone): There are other cases that are not yet handled yet because they have not come up yet.
// Example: `int(a) < float(3.3)` is the same as `int(a) <= int(3)`.
template <typename Output, typename Input>
std::optional<std::pair<PredicateCondition, Output>> lossless_predicate_cast(const PredicateCondition condition,
                                                                             const Input input) {
  using ResultPair = std::pair<PredicateCondition, Output>;

  if (const auto losslessly_casted_value = lossless_cast<Output>(input)) {
    // If the input value can be represented in the output type, we don't need to do anything
    return ResultPair{condition, *losslessly_casted_value};
  }

  if (!is_binary_numeric_predicate_condition(condition)) return std::nullopt;

  if constexpr (std::is_same_v<Input, double> && std::is_same_v<Output, float>) {
    // Equal comparisons where the value to compare to is a double that cannot be expressed as a float are a case that
    // we do not wish to handle for now.
    if (condition == PredicateCondition::Equals) return std::nullopt;

    // Let prev(double input) be the largest float that is smaller than input
    // x < input <=> x <= prev(input)
    // x <= input <=> x <= prev(input) (because there is no representable value between prev(input) and input)
    if (condition == PredicateCondition::LessThan || condition == PredicateCondition::LessThanEquals) {
      const auto adjusted_input = next_float_towards(input, std::numeric_limits<double>::lowest());
      if (!adjusted_input) return std::nullopt;
      return ResultPair{PredicateCondition::LessThanEquals, *adjusted_input};
    }

    // x > input <=> x >= next(input)
    // x >= input <=> x >= next(input) (because there is no representable value between input and next(input))
    if (condition == PredicateCondition::GreaterThan || condition == PredicateCondition::GreaterThanEquals) {
      const auto adjusted_input = next_float_towards(input, std::numeric_limits<double>::max());
      if (!adjusted_input) return std::nullopt;
      return ResultPair{PredicateCondition::GreaterThanEquals, *adjusted_input};
    }
  }

  return std::nullopt;
}

std::optional<std::pair<PredicateCondition, AllTypeVariant>> lossless_predicate_variant_cast(
    const PredicateCondition condition, const AllTypeVariant& variant, DataType target_data_type);
}  // namespace opossum
