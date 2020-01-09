
#include <storage/base_encoded_segment.hpp>
#include "string"
#include "fstream"

#include "table_export.hpp"

namespace opossum {
    TableExport::TableExport(const std::string &path_to_dir) : _path_to_dir(path_to_dir) {
      //TODO Check if file already exists;
      _create_table_meta_file();
      _create_column_meta_file();
    }

    void TableExport::export_table(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const {
        _append_to_file(_get_table_meta_relative_path(), _export_table_meta_data(table_wrapper));
        _append_to_file(_get_column_meta_relative_path(), _export_column_meta_data(table_wrapper));
    }

    std::string TableExport::_get_table_meta_header() const {
      std::stringstream table_meta_header;

      table_meta_header << "TABLE_NAME" << _separator;
      table_meta_header << "ROW_COUNT" << _separator;
      table_meta_header << "CHUNK_SIZE" << "\n";

      return table_meta_header.str();
    }

    std::string TableExport::_get_column_meta_header() const {
      std::stringstream column_meta_header;

      column_meta_header << "TABLE_NAME" << _separator;
      column_meta_header << "COLUMN_NAME" << _separator;
      column_meta_header << "COLUMN_DATA_TYPE" << _separator;
      column_meta_header << "ENCODING_TYPE" << "\n";

      return column_meta_header.str();
    }

    void TableExport::_create_table_meta_file() const {
      _append_to_file(_get_table_meta_relative_path(), _get_table_meta_header());
    }

    void TableExport::_create_column_meta_file() const {
      _append_to_file(_get_column_meta_relative_path(), _get_column_meta_header());
    }

    void TableExport::_append_to_file(const std::string& path,const std::string& str) const {
      std::fstream column_meta;
      column_meta.open(path, std::ofstream::out | std::ofstream::app);
      column_meta << str;
      column_meta.close();
    }

    //TODO Rewrite this
    const std::string TableExport::_get_table_meta_relative_path() const {
      return _path_to_dir + "/" + _table_meta_file_name + ".csv";
    }

    const std::string TableExport::_get_column_meta_relative_path() const {
      return _path_to_dir + "/" + _column_meta_file_name + ".csv";
    }

    std::string TableExport::_export_table_meta_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const {
      std::stringstream ss;

      ss << table_wrapper->get_name() << _separator;
      ss << table_wrapper->get_table()->row_count() << _separator;
      ss << table_wrapper->get_table()->max_chunk_size() << "\n";

      return ss.str();
    }

    std::string TableExport::_export_column_meta_data(std::shared_ptr<const CalibrationTableWrapper> table_wrapper) const {
      std::stringstream ss;

      auto const table = table_wrapper->get_table();
      int column_count = table->column_count();
      const auto column_names = table->column_names();

      for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        ss << table_wrapper->get_name() << _separator;
        ss << table->column_name(column_id) << _separator;
        ss << table->column_data_type(column_id) << _separator;

        auto const segment =  table->get_chunk(ChunkID {0})->get_segment(column_id);
        auto const encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment);

        auto const encoded_type_string = encoded_segment != nullptr ? encoded_segment->encoding_type() : EncodingType::Unencoded; //TODO in if and loop though chunk_id
        ss << encoded_type_string << "\n";
      }

      return ss.str();
    }
}
