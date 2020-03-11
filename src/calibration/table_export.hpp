#pragma once

#include "string"
#include "storage/table.hpp"
#include "calibration_table_wrapper.hpp"
#include "csv_writer.hpp"

namespace opossum {

enum TableExportType {
    TABLE, COLUMN, SEGMENT
};

class TableExport{
 public:
    TableExport(const std::string& path_to_dir);

    void export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);

private:
  const std::string& _path_to_dir;

  const std::string _table_meta_file_name = "table_meta";
  const std::string _column_meta_file_name = "column_meta";
  const std::string _segment_meta_file_name = "segment_meta";

  const std::vector<const std::string> _get_header(const TableExportType type) const { //TODO How to make this constexpr?
    switch (type){
      case TableExportType::TABLE: return std::vector<const std::string>({"TABLE_NAME", "ROW_COUNT", "CHUNK_SIZE"});
      case TableExportType::COLUMN: return std::vector<const std::string>({"TABLE_NAME", "COLUMN_NAME", "COLUMN_DATA_TYPE"});
      case TableExportType::SEGMENT: return std::vector<const std::string>({"TABLE_NAME", "COLUMN_NAME", "CHUNK_ID", "ENCODING_TYPE", "COMPRESSION_TYPE"});
    }
    throw std::runtime_error("Requested header for unknown TableExportType.");
  }

  CSVWriter _table_csv_writer = CSVWriter(_path_to_dir + "/" + _table_meta_file_name + ".csv", _get_header(TableExportType::TABLE));
  CSVWriter _column_csv_writer = CSVWriter(_path_to_dir + "/" + _column_meta_file_name + ".csv", _get_header(TableExportType::COLUMN));
  CSVWriter _segment_csv_writer = CSVWriter(_path_to_dir + "/" + _segment_meta_file_name + ".csv", _get_header(TableExportType::SEGMENT));

  void _export_table_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_column_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
  void _export_segment_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper);
};
}