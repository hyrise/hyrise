#include "calibration_table_generator.hpp"
#include "storage/table.hpp"

namespace opossum {

CalibrationTableGenerator::CalibrationTableGenerator(std::shared_ptr<TableGeneratorConfig> config) {
  // Generate all possible permutations of column types
  for (DataType data_type : config->data_types) {
    for (EncodingType encoding_type : config->encoding_types) {
      if (encoding_supports_data_type(encoding_type, data_type)) {
        for (ColumnDataDistribution column_data_distribution : config->column_data_distributions) {
          std::stringstream column_name_stringstream;
          column_name_stringstream << data_type << "_" << encoding_type << "_" << column_data_distribution;

          auto column_name = column_name_stringstream.str();

          _column_data_distributions.emplace_back(column_data_distribution);
          _column_specs.emplace_back(
              ColumnSpecification(column_data_distribution, data_type, encoding_type, column_name));
        }
      }  // if encoding is supported
    }
  }

  _config = config;
}

std::vector<std::shared_ptr<const CalibrationTableWrapper>> CalibrationTableGenerator::generate() const {
  auto table_wrappers = std::vector<std::shared_ptr<const CalibrationTableWrapper>>();
  table_wrappers.reserve(_config->chunk_sizes.size() * _config->row_counts.size());

  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  for (int chunk_size : _config->chunk_sizes) {
    for (int row_count : _config->row_counts) {
      const auto table = table_generator->generate_table(_column_specs, row_count, chunk_size, UseMvcc::Yes);

      const std::string table_name = std::to_string(chunk_size) + "_" + std::to_string(row_count);

      const auto calibration_table_wrapper = std::make_shared<const CalibrationTableWrapper>(
          CalibrationTableWrapper(table, table_name, _column_data_distributions));
      table_wrappers.emplace_back(calibration_table_wrapper);
    }
  }
  return table_wrappers;
}
}  // namespace opossum
