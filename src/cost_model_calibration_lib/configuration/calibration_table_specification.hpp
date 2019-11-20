#pragma once

#include <nlohmann/json.hpp>
#include <map>
#include <string>

#include "calibration_column_specification.hpp"

namespace opossum {

struct CalibrationTableSpecification {
  std::string table_path;
  std::string table_name;
  int table_size;
};

inline void to_json(nlohmann::json& j, const CalibrationTableSpecification& s) {
  j = nlohmann::json{{"table_path", s.table_path}, {"table_name", s.table_name}, {"table_size", s.table_size}};
}

inline void from_json(const nlohmann::json& j, CalibrationTableSpecification& s) {
  s.table_path = j.at("table_path").get<std::string>();
  s.table_name = j.at("table_name").get<std::string>();
  s.table_size = j.at("table_size").get<int>();
}

}  // namespace opossum
