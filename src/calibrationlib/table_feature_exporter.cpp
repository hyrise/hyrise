#include "fstream"
#include "string"

#include <storage/base_encoded_segment.hpp>
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "table_feature_exporter.hpp"

namespace opossum {
TableFeatureExporter::TableFeatureExporter(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {}

void TableFeatureExporter::export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  _export_table_data(table_wrapper);
  _export_column_data(table_wrapper);
  _export_segment_data(table_wrapper);
}

void TableFeatureExporter::_export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  const auto table_name = table_wrapper->get_name();
  const auto row_count = table_wrapper->get_table()->row_count();
  const auto chunk_size = table_wrapper->get_table()->target_chunk_size();

  _table_csv_writer.set_value("TABLE_NAME", table_name);
  _table_csv_writer.set_value("ROW_COUNT", row_count);
  _table_csv_writer.set_value("CHUNK_SIZE", chunk_size);

  _table_csv_writer.write_row();
}

void TableFeatureExporter::_export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  auto const table = table_wrapper->get_table();
  int column_count = table->column_count();
  const auto column_names = table->column_names();

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto table_name = table_wrapper->get_name();
    const auto column_name = table->column_name(column_id);
    const auto column_data_type = table->column_data_type(column_id);

    _column_csv_writer.set_value("TABLE_NAME", table_name);
    _column_csv_writer.set_value("COLUMN_NAME", column_name);
    _column_csv_writer.set_value("COLUMN_DATA_TYPE", column_data_type);

    _column_csv_writer.write_row();
  }
}

void TableFeatureExporter::_export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) {
  const auto table = table_wrapper->get_table();
  const auto table_name = table_wrapper->get_name();
  const auto chunk_count = table->chunk_count();
  const auto column_count = table->column_count();

  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    const auto column_name = table->column_name(column_id);

    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      _segment_csv_writer.set_value("TABLE_NAME", table_name);
      _segment_csv_writer.set_value("COLUMN_NAME", column_name);
      _segment_csv_writer.set_value("CHUNK_ID", chunk_id);

      auto const segment = table->get_chunk(chunk_id)->get_segment(column_id);

      // if segment is encoded write out values
      if (const auto encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment)) {
        // Encoding Type
        _segment_csv_writer.set_value("ENCODING_TYPE", encoded_segment->encoding_type());

        // Compressed Vector Type
        if (const auto compressed_vector_type = encoded_segment->compressed_vector_type()) {
          _segment_csv_writer.set_value("COMPRESSION_TYPE", *compressed_vector_type);
        } else {
          _segment_csv_writer.set_value("COMPRESSION_TYPE", "null");
        }
        // if segment is not encoded write default values for chunk;
      } else {
        _segment_csv_writer.set_value("ENCODING_TYPE", EncodingType::Unencoded);
        _segment_csv_writer.set_value("COMPRESSION_TYPE", "null");
      }
      _segment_csv_writer.write_row();
    }
  }
}
}  // namespace opossum
