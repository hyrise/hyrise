#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationColumnSpecification {
  DataType type;
  std::string value_distribution;
  bool sorted;
  uint16_t distinct_values;
  EncodingType encoding;
};

inline void to_json(nlohmann::json& j, const CalibrationColumnSpecification& s) {
  j = nlohmann::json{
      {"type", s.type},         {"value_distribution", s.value_distribution},
      {"sorted", s.sorted},     {"distinct_values", s.distinct_values},
      {"encoding", s.encoding},
  };
}

inline bool operator==(const CalibrationColumnSpecification& lhs, const CalibrationColumnSpecification& rhs)
{
  return std::tie(lhs.type, lhs.value_distribution, lhs.sorted, lhs.distinct_values, lhs.encoding) ==
         std::tie(rhs.type, rhs.value_distribution, rhs.sorted, rhs.distinct_values, rhs.encoding);
}

inline void from_json(const nlohmann::json& j, CalibrationColumnSpecification& s) {
  auto data_type_string = j.value("type", "int");
  if (data_type_to_string.right.find(data_type_string) == data_type_to_string.right.end()) {
      Fail("Unsupported data type");
  }
  s.type = data_type_to_string.right.at(data_type_string);
  s.value_distribution = j.value("value_distribution", "uniform");
  s.sorted = j.value("sorted", false);
  s.distinct_values = j.value("distinct_values", 100);
  auto encoding_string = j.value("encoding", "Unencoded");
  if (encoding_type_to_string.right.find(encoding_string) == encoding_type_to_string.right.end()) {
    Fail("Unsupported encoding");
  }
  s.encoding = encoding_type_to_string.right.at(encoding_string);
}

}  // namespace opossum
