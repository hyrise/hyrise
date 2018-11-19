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

  const auto encoding_strings = j.at("encodings").get<std::vector<std::string>>();

  std::vector<EncodingType> encodings{};
  for (const auto& encoding_string : encoding_strings) {
    if (encoding_type_to_string.right.find(encoding_string) == encoding_type_to_string.right.end()) {
      Fail("Unsupported encoding");
    }
    encodings.push_back(encoding_type_to_string.right.at(encoding_string));
  }
  configuration.encodings = encodings;

  const auto data_type_strings = j.at("data_types").get<std::vector<std::string>>();
  std::vector<DataType> data_types{};
  for (const auto& data_type_string : data_type_strings) {
    if (data_type_to_string.right.find(data_type_string) == data_type_to_string.right.end()) {
      Fail("Unsupported data type");
    }
    data_types.push_back(data_type_to_string.right.at(data_type_string));
  }
  configuration.data_types = data_types;

  configuration.selectivities = j.at("selectivities").get<std::vector<float>>();
  configuration.columns = j.at("columns").get<std::vector<CalibrationColumnSpecification>>();
}

}  // namespace opossum
