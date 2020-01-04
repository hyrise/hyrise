//
// Created by Lukas Böhme on 17.12.19.
//

#include "table_generator.hpp"
#include "storage/table.hpp"
#include "constant_mappings.hpp"

namespace opossum {

  TableGenerator::TableGenerator(std::shared_ptr<TableGeneratorConfig> config) {
      // Generate all possible permutations of column types
    for (DataType data_type : config->data_types){
      for (EncodingType encoding_type : config->encoding_types){
        if (encoding_supports_data_type(encoding_type, data_type)){
          for (ColumnDataDistribution column_data_distribution : config->column_data_distribution){
              data_types_collection.emplace_back(data_type);
              segment_encoding_spec_collection.emplace_back(SegmentEncodingSpec(encoding_type));
              column_data_distribution_collection.emplace_back(column_data_distribution);

              std::stringstream column_name_str;

              column_name_str << data_type << "_"
                              << encoding_type << "_"
                              << column_data_distribution;

              column_names.emplace_back(column_name_str.str());
          }
        }  // if encoding is supported
      }

    }

    chunk_offsets.assign(config->chunk_offsets.begin(), config->chunk_offsets.end());
    row_counts.assign(config->row_counts.begin(), config->row_counts.end());
  }

  std::vector<std::shared_ptr<const CalibrationTableWrapper>> TableGenerator::generate() const {
    auto table_wrapper = std::vector<std::shared_ptr<const CalibrationTableWrapper>>();
    table_wrapper.reserve(chunk_offsets.size() * row_counts.size());

    auto table_generator = std::make_shared<SyntheticTableGenerator>();

    for (int chunk_size : chunk_offsets) {
      for (int row_count : row_counts){
        const auto table = table_generator->generate_table(
                  column_data_distribution_collection,
                  data_types_collection,
                  row_count,
                  chunk_size,
                  {segment_encoding_spec_collection},
                  {column_names},
                  UseMvcc::Yes);    // MVCC = Multiversion concurrency control

        const auto calibration_table_wrapper = std::make_shared<const CalibrationTableWrapper>(CalibrationTableWrapper(
                table,
                column_data_distribution_collection));
        table_wrapper.emplace_back(calibration_table_wrapper);
      }
    }
    return table_wrapper;
  }
}  // namespace opossum
