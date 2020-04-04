#pragma once

#include <string>

#include "calibration_table_wrapper.hpp"
#include "csv_writer.hpp"
#include "storage/table.hpp"

namespace opossum {

enum TableFeatureExportType { TABLE, COLUMN, SEGMENT };

class TableFeatureExporter {
 public:
  explicit TableFeatureExporter(const std::string& path_to_dir);

  void export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);

 private:
  const std::string& _path_to_dir;

  const std::string _table_meta_file_name = "table_meta";
  const std::string _column_meta_file_name = "column_meta";
  const std::string _segment_meta_file_name = "segment_meta";

  const std::vector<std::string> _get_header(const TableFeatureExportType type) const {
    switch (type) {
      case TableFeatureExportType::TABLE:
        return std::vector<std::string>({"TABLE_NAME", "ROW_COUNT", "CHUNK_SIZE"});
      case TableFeatureExportType::COLUMN:
        return std::vector<std::string>({"TABLE_NAME", "COLUMN_NAME", "COLUMN_DATA_TYPE"});
      case TableFeatureExportType::SEGMENT:
        return std::vector<std::string>({"TABLE_NAME", "COLUMN_NAME", "CHUNK_ID", "ENCODING_TYPE", "COMPRESSION_TYPE"});
    }
    throw std::runtime_error("Requested header for unknown TableFeatureExportType.");
  }

  CSVWriter _table_csv_writer =
      CSVWriter(_path_to_dir + "/" + _table_meta_file_name + ".csv", _get_header(TableFeatureExportType::TABLE));
  CSVWriter _column_csv_writer =
      CSVWriter(_path_to_dir + "/" + _column_meta_file_name + ".csv", _get_header(TableFeatureExportType::COLUMN));
  CSVWriter _segment_csv_writer =
      CSVWriter(_path_to_dir + "/" + _segment_meta_file_name + ".csv", _get_header(TableFeatureExportType::SEGMENT));

  void _export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
};
}  // namespace opossum
