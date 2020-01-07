//
// Created by Lukas Böhme on 30.12.19.
//

#include <operators/table_wrapper.hpp>
#include <operators/export_csv.hpp>
#include "calibration_table_wrapper.hpp"

namespace opossum {
  CalibrationTableWrapper::CalibrationTableWrapper(const std::shared_ptr<Table> table,
                                                   const std::string& table_name,
                                                   const std::vector<ColumnDataDistribution> column_data_distribution_collection)
                                                   : _table(table),
                                                     _name(table_name),
                                                     _column_data_distribution_collection(column_data_distribution_collection){
    assert(table->column_count() == column_data_distribution_collection.size());
  }

  void CalibrationTableWrapper::export_table_meta_data(const std::string &path) const {
    //TODO Copy Duplicate. How do I export meta data only?
    CsvMeta meta{};
    // Column Types
    for (ColumnID column_id{0}; column_id < _table->column_count(); ++column_id) {
      ColumnMeta column_meta;
      column_meta.name = _table->column_name(column_id);
      column_meta.type = data_type_to_string.left.at(_table->column_data_type(column_id));
      column_meta.nullable = _table->column_is_nullable(column_id);

      meta.columns.push_back(column_meta);
    }

    nlohmann::json meta_json = meta;

    std::ofstream meta_file_stream(path + "/" + _name + CsvMeta::META_FILE_EXTENSION);
    meta_file_stream << std::setw(4) << meta_json << std::endl;
  }

    const std::shared_ptr<Table> CalibrationTableWrapper::get_table() const {
    return _table;
  }

  const std::string &CalibrationTableWrapper::get_name() const {
    return _name;
  }

  const ColumnDataDistribution CalibrationTableWrapper::get_column_data_distribution(ColumnID id) const {
    return _column_data_distribution_collection[id];
  }
}