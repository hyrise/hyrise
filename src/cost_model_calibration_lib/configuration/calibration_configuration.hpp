#pragma once

#include <json.hpp>
#include <string>
#include <vector>

#include "calibration_table_specification.hpp"

namespace opossum {

struct CalibrationConfiguration {
  // TBL-based table creation
  std::vector<CalibrationTableSpecification> table_specifications;

  // On-the-fly table creation
  bool table_generation;
  std::string table_generation_name_prefix;
  std::vector<size_t> table_generation_table_sizes;

  std::string output_path;
  std::string tpch_output_path;
  size_t calibration_runs;
  std::vector<EncodingType> encodings;
  bool calibrate_vector_compression_types;
  std::vector<DataType> data_types;
  std::vector<float> selectivities;
  bool use_scan;
  bool run_tpch;
  std::vector<CalibrationColumnSpecification> columns;
};

inline void to_json(nlohmann::json& j, const CalibrationConfiguration& s) {
  j = nlohmann::json{{"output_path", s.output_path},
                     {"tpch_output_path", s.tpch_output_path},
                     {"calibration_runs", s.calibration_runs},
                     {"table_specifications", s.table_specifications},
                     {"table_name_prefix", s.table_generation_name_prefix},
                     {"table_generation_table_sizes", s.table_generation_table_sizes},
                     {"encodings", s.encodings},
                     {"calibrate_vector_compression_types", s.calibrate_vector_compression_types},
                     {"data_types", s.data_types},
                     {"selectivities", s.selectivities},
                     {"use_scan", s.use_scan},
                     {"run_tpch", s.run_tpch},
                     {"columns", s.columns}};
}

inline void from_json(const nlohmann::json& j, CalibrationConfiguration& configuration) {
  if (j.find("table_generation") != j.end()) {
    configuration.table_generation = true;
    const auto table_generation_config = j.at("table_generation");
    configuration.table_generation_name_prefix = table_generation_config.at("table_name_prefix");

    const auto table_sizes = table_generation_config.at("table_sizes").get<std::vector<size_t>>();
    for (const auto table_size : table_sizes) {
      configuration.table_generation_table_sizes.push_back(table_size);
    }
  } else {
    configuration.table_generation = false;
  }

  configuration.output_path = j.value("output_path", "./calibration_results.json");
  configuration.tpch_output_path = j.value("tpch_output_path", "./tpch_calibration_results.json");
  configuration.calibration_runs = j.value("calibration_runs", 1000u);
  configuration.table_specifications = j.value("table_specifications", std::vector<CalibrationTableSpecification>{});
  configuration.calibrate_vector_compression_types = j.value("calibrate_vector_compression_types", false);
  configuration.use_scan = j.value("use_scan", true);
  configuration.run_tpch = j.value("run_tpch", false);

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
  std::vector<DataType> calibration_data_types{};
  for (const auto& data_type_string : data_type_strings) {
    if (data_type_to_string.right.find(data_type_string) == data_type_to_string.right.end()) {
      Fail("Unsupported data type");
    }
    calibration_data_types.push_back(data_type_to_string.right.at(data_type_string));
  }
  configuration.data_types = calibration_data_types;

  configuration.selectivities = j.at("selectivities").get<std::vector<float>>();
  configuration.columns = j.at("columns").get<std::vector<CalibrationColumnSpecification>>();
}

}  // namespace opossum
