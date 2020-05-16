#include "table_feature_exporter.hpp"

#include <fstream>
#include <string>

#include "constant_mappings.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"

namespace opossum {
TableFeatureExporter::TableFeatureExporter(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {}

void TableFeatureExporter::export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  _export_table_data(table_wrapper);
  _export_column_data(table_wrapper);
  _export_segment_data(table_wrapper);
}

void TableFeatureExporter::flush() {
  for (auto& [export_type, table] : _tables) {
    std::stringstream path;
    path << _path_to_dir << "/" << _table_names.at(export_type) << "_new.csv";
    CsvWriter::write(*table, path.str());
    _tables[export_type] = std::make_shared<Table>(_column_definitions.at(export_type), TableType::Data);
  }
}

void TableFeatureExporter::_export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  const auto table_name = pmr_string{table_wrapper->get_name()};
  const auto row_count = static_cast<int64_t>(table_wrapper->get_table()->row_count());
  const auto chunk_size = static_cast<int32_t>(table_wrapper->get_table()->target_chunk_size());

  _tables[TableFeatureExportType::TABLE]->append({table_name, row_count, chunk_size});
}

void TableFeatureExporter::_export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  auto const table = table_wrapper->get_table();
  int column_count = table->column_count();
  const auto column_names = table->column_names();

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto table_name = pmr_string{table_wrapper->get_name()};
    const auto column_name = pmr_string{table->column_name(column_id)};
    const auto column_data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};

    _tables[TableFeatureExportType::COLUMN]->append({table_name, column_name, column_data_type});
  }
}

void TableFeatureExporter::_export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  const auto table = table_wrapper->get_table();
  const auto table_name = pmr_string{table_wrapper->get_name()};
  const auto chunk_count = table->chunk_count();
  const auto column_count = table->column_count();

  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    const auto column_name = pmr_string{table->column_name(column_id)};

    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      auto const segment = table->get_chunk(chunk_id)->get_segment(column_id);
      AllTypeVariant encoding_type = {pmr_string{encoding_type_to_string.left.at(EncodingType::Unencoded)}};
      AllTypeVariant compression_type = NULL_VALUE;
      // if segment is encoded write out values
      if (const auto encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment)) {
        // Encoding Type
        encoding_type = AllTypeVariant{pmr_string{encoding_type_to_string.left.at(encoded_segment->encoding_type())}};

        // Compressed Vector Type
        if (const auto compressed_vector_type = encoded_segment->compressed_vector_type()) {
          std::stringstream ss;
          ss << *compressed_vector_type;
          compression_type = pmr_string{ss.str()};
        }
      }
      _tables[TableFeatureExportType::SEGMENT]->append(
          {table_name, column_name, static_cast<int32_t>(chunk_id), encoding_type, compression_type});
    }
  }
}
}  // namespace opossum
