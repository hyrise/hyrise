#include "string"
#include "storage/table.hpp"
#include "calibration_table_wrapper.hpp"

namespace opossum {
class TableExport{
 public:
    TableExport(const std::string& path_to_dir);

    void export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const;

private:
  const std::string& _path_to_dir;

  const std::string _table_meta_file_name = "table_meta";
  const std::string _column_meta_file_name = "column_meta";
  const std::string _chunk_meta_file_name = "chunk_meta";

  const std::string _separator = ",";

  std::string _get_table_meta_header() const;
  std::string _get_column_meta_header() const;
  std::string _get_chunk_meta_header() const;

  const std::string _get_table_meta_relative_path() const ;
  const std::string _get_column_meta_relative_path() const;
  const std::string _get_chunk_meta_relative_path() const;

  void _create_table_meta_file() const;
  void _create_column_meta_file() const;
  void _create_chunk_meta_file() const;

  std::string _export_table_meta_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const;
  std::string _export_column_meta_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const ;
  std::string _export_chunk_meta_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const ;

  void _append_to_file(const std::string &path, const std::string &str) const;
};
}