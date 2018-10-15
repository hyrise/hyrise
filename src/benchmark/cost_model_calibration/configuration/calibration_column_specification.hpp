#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationColumnSpecification {
  std::string type;
  std::string value_distribution;
  bool sorted;
  int distinct_values;
  EncodingType encoding;
};

inline void to_json(nlohmann::json& j, const CalibrationColumnSpecification& s) {
  j = nlohmann::json{
      {"type", s.type},
      {"value_distribution", s.value_distribution},
      {"sorted", s.sorted},
      {"distinct_values", s.distinct_values},
      {"encoding", s.encoding},
  };
}

inline void from_json(const nlohmann::json& j, CalibrationColumnSpecification& s) {
  s.type = j.value("type", "int");
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
