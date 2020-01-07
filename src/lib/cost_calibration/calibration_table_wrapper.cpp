//
// Created by Lukas Böhme on 30.12.19.
//

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

  std::string CalibrationTableWrapper::export_table_meta_data() const {
    std::stringstream ss;
    const auto _separator = ",";

    ss << _name << _separator;
    ss << _table->row_count() << _separator;
    ss << _table->max_chunk_size() << "\n";

    return ss.str();
  }

  std::string CalibrationTableWrapper::export_column_meta_data() const {
    std::stringstream ss;
    const auto _separator = ",";
    int column_count = _table->column_count();
    const auto column_names = _table->column_names();

    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      ss << _name << _separator;
      ss << _table->column_name(column_id) << _separator;
      ss << _table->column_data_type(column_id) << "\n";
      //ss << _table->get_chunk(ChunkID(0))->get_segment(column_id); //TODO How to get segment
    }

    return ss.str();
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