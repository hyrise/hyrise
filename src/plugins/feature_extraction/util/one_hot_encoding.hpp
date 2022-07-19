#pragma once

#include "feature_extraction/feature_types.hpp"

#include <magic_enum.hpp>


namespace opossum {

template <typename EnumType>
std::shared_ptr<std::vector<Feature>> one_hot_encoding(const EnumType value) {
  auto result = std::make_shared<std::vector<Feature>>(magic_enum::enum_count<EnumType>());
  const auto& index = magic_enum::enum_index(value);
  Assert(index, "No index found for '" + std::string{magic_enum::enum_name(value)} + "'");
  (*result)[*index] = Feature{1};
  return result;
}

template <typename EnumType>
std::vector<std::string> one_hot_headers(const std::string& prefix) {
  const auto& enum_names = magic_enum::enum_names<EnumType>();
  auto result = std::vector<std::string>{};
  result.reserve(enum_names.size());
  for (const auto& entry_name : enum_names) {
    result.emplace_back(prefix + std::string{entry_name});
  }
  return result;
}

}  // namespace opossum
