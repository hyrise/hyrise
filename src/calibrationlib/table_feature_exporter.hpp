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

  const std::map<TableFeatureExportType, std::vector<std::string>> headers = {
      {TableFeatureExportType::TABLE, {"TABLE_NAME", "ROW_COUNT", "CHUNK_SIZE"}},
      {TableFeatureExportType::COLUMN, {"TABLE_NAME", "COLUMN_NAME", "COLUMN_DATA_TYPE"}},
      {TableFeatureExportType::SEGMENT,
       {"TABLE_NAME", "COLUMN_NAME", "CHUNK_ID", "ENCODING_TYPE", "COMPRESSION_TYPE"}}};

 private:
  const std::string& _path_to_dir;

  const std::string _table_meta_file_name = "table_meta";
  const std::string _column_meta_file_name = "column_meta";
  const std::string _segment_meta_file_name = "segment_meta";

  CSVWriter _table_csv_writer =
      CSVWriter(_path_to_dir + "/" + _table_meta_file_name + ".csv", headers.at(TableFeatureExportType::TABLE));
  CSVWriter _column_csv_writer =
      CSVWriter(_path_to_dir + "/" + _column_meta_file_name + ".csv", headers.at(TableFeatureExportType::COLUMN));
  CSVWriter _segment_csv_writer =
      CSVWriter(_path_to_dir + "/" + _segment_meta_file_name + ".csv", headers.at(TableFeatureExportType::SEGMENT));

  void _export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
};
}  // namespace opossum
