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
};

class CalibrationTableGenerator {
 public:
  explicit CalibrationTableGenerator(std::shared_ptr<TableGeneratorConfig> config);
  std::vector<std::shared_ptr<const CalibrationTableWrapper>> generate() const;

 private:
  std::shared_ptr<TableGeneratorConfig> _config;
  std::vector<ColumnDataDistribution> _column_data_distributions;
  std::vector<ColumnSpecification> _column_specs;
};

}  // namespace opossum
