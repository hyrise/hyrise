#pragma once

#include <iostream>

#include <all_type_variant.hpp>
#include <storage/encoding_type.hpp>
#include <synthetic_table_generator.hpp>
#include "calibration_table_wrapper.hpp"

class Table;

namespace opossum {

struct TableGeneratorConfig {
  std::set<DataType> data_types;
  std::set<EncodingType> encoding_types;
  std::vector<ColumnDataDistribution> column_data_distributions;
  std::set<ChunkOffset> chunk_sizes;
  std::set<int> row_counts;
  bool generate_sorted_tables;
  bool generate_foreign_key_tables;
};

class CalibrationTableGenerator {
 public:
  explicit CalibrationTableGenerator(std::shared_ptr<TableGeneratorConfig> config);
  std::vector<std::shared_ptr<const CalibrationTableWrapper>> generate() const;

 private:
  std::shared_ptr<const CalibrationTableWrapper> _generate_sorted_table(
      const std::shared_ptr<const CalibrationTableWrapper>& original_table) const;
  std::shared_ptr<const CalibrationTableWrapper> _generate_foreign_key_table(
      const std::shared_ptr<const CalibrationTableWrapper>& original_table,
      const std::shared_ptr<const SyntheticTableGenerator>& table_generator) const;
  std::shared_ptr<TableGeneratorConfig> _config;
  std::vector<ColumnDataDistribution> _column_data_distributions;
  std::vector<ColumnSpecification> _column_specs;
  size_t _foreign_key_threshold{3000};
};

}  // namespace opossum
