#pragma once

#include <nlohmann/json.hpp>
#include <chrono>
#include <string>
#include <fstream>
#include <vector>

#include <boost/asio/ip/host_name.hpp>

#include "calibration_table_specification.hpp"
#include "../calibration_helper.hpp"

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
  std::vector<size_t> distinct_value_counts = {10, 10'000, 1'000'000};
  bool calibrate_scans;
  bool calibrate_joins;
  bool run_tpch;

  std::vector<CalibrationColumnSpecification> columns;
  std::vector<std::string> generated_tables;
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
                     {"calibrate_scans", s.calibrate_scans},
                     {"calibrate_joins", s.calibrate_joins},
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
      configuration.generated_tables.push_back(configuration.table_generation_name_prefix + std::to_string(table_size));
    }
  } else {
    configuration.table_generation = false;
  }


  auto now_iso_date = get_time_as_iso_string(std::chrono::system_clock::now());
  auto host_name = boost::asio::ip::host_name();

  auto output_path = j.value("output_path", "./calibration_results");
  output_path += "_" + host_name + "_" + now_iso_date + ".csv";

  auto tpch_output_path = j.value("tpch_output_path", "./tpch_calibration_results");
  tpch_output_path += "_" + host_name + "_" + now_iso_date + ".csv";

  configuration.output_path = output_path;
  configuration.tpch_output_path = tpch_output_path;
  configuration.calibration_runs = j.value("calibration_runs", 1000u);
  configuration.table_specifications = j.value("table_specifications", std::vector<CalibrationTableSpecification>{});
  configuration.calibrate_vector_compression_types = j.value("calibrate_vector_compression_types", false);
  configuration.calibrate_scans = j.value("calibrate_scans", true);
  configuration.calibrate_joins = j.value("calibrate_joins", false);
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

  std::vector<SegmentEncodingSpec> segments_encodings{};
  for (const auto& encoding : configuration.encodings) {
    if (encoding != EncodingType::Unencoded) {
      auto encoder = create_encoder(encoding);

      if (configuration.calibrate_vector_compression_types && encoder->uses_vector_compression()) {
        for (const auto& vector_compression :
             {VectorCompressionType::FixedSizeByteAligned, VectorCompressionType::SimdBp128}) {
          segments_encodings.push_back(SegmentEncodingSpec{encoding, vector_compression});
        }
      } else {
        segments_encodings.push_back(SegmentEncodingSpec{encoding});
      }
    } else {
      segments_encodings.push_back(SegmentEncodingSpec{EncodingType::Unencoded});
    }
  }

  auto column_id = ColumnID{0};
  std::vector<CalibrationColumnSpecification> column_specs;
  for (const auto& data_type : configuration.data_types) {
    for (const auto& encoding_spec : segments_encodings) {
      if (encoding_supports_data_type(encoding_spec.encoding_type, data_type)) {
	// Due to a lack of functionality, LZ4 needs to be manually cleaned. In the long run, LZ4 shall no longer use vetor
	// compression.
	if (encoding_spec.encoding_type == EncodingType::LZ4 && encoding_spec.vector_compression_type &&
            *encoding_spec.vector_compression_type == VectorCompressionType::FixedSizeByteAligned) {
		continue;
	}
        // for every encoding, we create three columns that allow calibrating the query
        // `WHERE a between b and c` with a,b,c being columns encoded in the requested encoding type.
        for (const size_t distinct_value_count : configuration.distinct_value_counts) {
          CalibrationColumnSpecification column_spec;
          column_spec.column_id = column_id;
          column_spec.column_name = "column_" + std::to_string(column_id);
          column_spec.data_type = data_type;
          column_spec.value_distribution = "uniform";
          column_spec.sorted = false;
          column_spec.distinct_value_count = distinct_value_count;
          column_spec.encoding = encoding_spec;

          column_specs.push_back(column_spec);
          ++column_id;
        }
      }
    }
  }
  configuration.columns = column_specs;
}

}  // namespace opossum
