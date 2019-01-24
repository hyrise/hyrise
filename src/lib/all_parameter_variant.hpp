#pragma once

#include "all_type_variant.hpp"

namespace {
// https://stackoverflow.com/questions/52393831/can-i-extend-variant-in-c
template <typename T, typename... Args>
struct VariantConcatenator;

template <typename... Args0, typename... Args1>
struct VariantConcatenator<std::variant<Args0...>, Args1...> {
  using type = std::variant<Args0..., Args1...>;
};
}  // namespace

namespace opossum {

/**
 * AllParameterVariant holds either an AllTypeVariant, a ColumnID or a Placeholder.
 * It should be used to generalize Opossum operator calls.
 */

// AllTypeVariant must be named first, because we would otherwise break `is_variant` below
using AllParameterVariant = VariantConcatenator<AllTypeVariant, ColumnID, ParameterID>::type;

// Function to check if AllParameterVariant is AllTypeVariant
inline bool is_variant(const AllParameterVariant& variant) {
  return variant.index() < std::variant_size_v<AllTypeVariant>;
}

// Function to check if AllParameterVariant is a column id
inline bool is_column_id(const AllParameterVariant& variant) { return (std::holds_alternative<ColumnID>(variant)); }

// Function to check if AllParameterVariant is a ParameterID
inline bool is_parameter_id(const AllParameterVariant& variant) {
  return (std::holds_alternative<ParameterID>(variant));
}

std::string to_string(const AllParameterVariant& x);

AllTypeVariant to_all_type_variant(const AllParameterVariant& variant);
AllParameterVariant to_all_parameter_variant(const AllTypeVariant& variant);

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::AllParameterVariant> {
  size_t operator()(const opossum::AllParameterVariant& all_type_variant) const;
};

std::ostream& operator<<(std::ostream&, const opossum::AllParameterVariant&);

}  // namespace std
