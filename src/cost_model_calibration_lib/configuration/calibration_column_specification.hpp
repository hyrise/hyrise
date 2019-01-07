#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationColumnSpecification {
  std::string column_name;
  DataType type;
  std::string value_distribution;
  bool sorted;
  uint distinct_values;
  EncodingType encoding;
  size_t fraction;
};

inline void to_json(nlohmann::json& j, const CalibrationColumnSpecification& s) {
  j = nlohmann::json{{"column_name", s.column_name},
                     {"type", s.type},
                     {"value_distribution", s.value_distribution},
                     {"sorted", s.sorted},
                     {"distinct_values", s.distinct_values},
                     {"encoding", s.encoding},
                     {"fraction", s.fraction}};
}

inline bool operator==(const CalibrationColumnSpecification& lhs, const CalibrationColumnSpecification& rhs) {
  return std::tie(lhs.column_name, lhs.type, lhs.value_distribution, lhs.sorted, lhs.distinct_values, lhs.encoding,
                  lhs.fraction) == std::tie(rhs.column_name, rhs.type, rhs.value_distribution, rhs.sorted,
                                            rhs.distinct_values, rhs.encoding, rhs.fraction);
}

inline void from_json(const nlohmann::json& j, CalibrationColumnSpecification& s) {
  auto data_type_string = j.value("type", "int");
  if (data_type_to_string.right.find(data_type_string) == data_type_to_string.right.end()) {
    Fail("Unsupported data type");
  }
  s.column_name = j.at("column_name").get<std::string>();
  s.type = data_type_to_string.right.at(data_type_string);
  s.value_distribution = j.value("value_distribution", "uniform");
  s.sorted = j.value("sorted", false);
  s.distinct_values = j.value("distinct_values", 0);
  auto encoding_string = j.value("encoding", "Unencoded");
  if (encoding_type_to_string.right.find(encoding_string) == encoding_type_to_string.right.end()) {
    Fail("Unsupported encoding");
  }
  s.encoding = encoding_type_to_string.right.at(encoding_string);
  s.fraction = j.value("fraction", 0);
}

}  // namespace opossum
