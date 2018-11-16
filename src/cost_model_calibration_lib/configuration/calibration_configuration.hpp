#pragma once

#include <json.hpp>
#include <string>
#include <vector>

#include "calibration_table_specification.hpp"

namespace opossum {

struct CalibrationConfiguration {
  std::vector<CalibrationTableSpecification> table_specifications;
  std::string output_path;
  std::string tpch_output_path;
  size_t calibration_runs;
  std::vector<EncodingType> encodings;
  std::vector<DataType> data_types;
  std::vector<float> selectivities;
  std::vector<CalibrationColumnSpecification> columns;
};

inline void to_json(nlohmann::json& j, const CalibrationConfiguration& s) {
  j = nlohmann::json{{"output_path", s.output_path},
                     {"tpch_output_path", s.tpch_output_path},
                     {"calibration_runs", s.calibration_runs},
                     {"table_specifications", s.table_specifications},
                     {"encodings", s.encodings},
                     {"data_types", s.data_types},
                     {"selectivities", s.selectivities},
                     {"columns", s.columns}};
}

inline void from_json(const nlohmann::json& j, CalibrationConfiguration& configuration) {
  configuration.output_path = j.value("output_path", "./calibration_results.json");
  configuration.tpch_output_path = j.value("tpch_output_path", "./tpch_calibration_results.json");
  configuration.calibration_runs = j.value("calibration_runs", 1000u);
  configuration.table_specifications = j.value("table_specifications", std::vector<CalibrationTableSpecification>{});
  configuration.encodings = j.value("encodings", std::vector<EncodingType>{});
  configuration.data_types = j.value("data_types", std::vector<DataType>{});
  configuration.selectivities = j.value("selectivities", std::vector<float>{});
  configuration.columns = j.value("columns", std::vector<CalibrationColumnSpecification>{});
}

}  // namespace opossum
