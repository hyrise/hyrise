#pragma once

#include <string>

#include "calibration_table_wrapper.hpp"
#include "storage/table.hpp"

namespace opossum {

enum TableFeatureExportType { TABLE, COLUMN, SEGMENT };

class TableFeatureExporter {
 public:
  explicit TableFeatureExporter(const std::string& path_to_dir);

  void export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);

  void flush();

 protected:
  std::map<TableFeatureExportType, std::shared_ptr<Table>> _tables = {
      {TableFeatureExportType::TABLE,
       std::make_shared<Table>(_column_definitions.at(TableFeatureExportType::TABLE), TableType::Data)},
      {TableFeatureExportType::COLUMN,
       std::make_shared<Table>(_column_definitions.at(TableFeatureExportType::COLUMN), TableType::Data)},
      {TableFeatureExportType::SEGMENT,
       std::make_shared<Table>(_column_definitions.at(TableFeatureExportType::SEGMENT), TableType::Data)}};

 private:
  static inline std::map<TableFeatureExportType, TableColumnDefinitions> _column_definitions = {
      {TableFeatureExportType::TABLE, TableColumnDefinitions{{"TABLE_NAME", DataType::String, false},
                                                             {"ROW_COUNT", DataType::Long, false},
                                                             {"CHUNK_SIZE", DataType::Int, false}}},
      {TableFeatureExportType::COLUMN, TableColumnDefinitions{{"TABLE_NAME", DataType::String, false},
                                                              {"COLUMN_NAME", DataType::String, false},
                                                              {"COLUMN_DATA_TYPE", DataType::String, false}}},
      {TableFeatureExportType::SEGMENT, TableColumnDefinitions{{"TABLE_NAME", DataType::String, false},
                                                               {"COLUMN_NAME", DataType::String, false},
                                                               {"CHUNK_ID", DataType::Int, false},
                                                               {"ENCODING_TYPE", DataType::String, false},
                                                               {"COMPRESSION_TYPE", DataType::String, false}}}};

  const std::map<TableFeatureExportType, std::string> _table_names = {
      {TableFeatureExportType::TABLE, "table_meta"},
      {TableFeatureExportType::COLUMN, "column_meta"},
      {TableFeatureExportType::SEGMENT, "segment_meta"}};

  const std::string _path_to_dir;

  void _export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
};
}  // namespace opossum
