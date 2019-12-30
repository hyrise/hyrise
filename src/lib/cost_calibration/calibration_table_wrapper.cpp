//
// Created by Lukas Böhme on 30.12.19.
//

#include "calibration_table_wrapper.hpp"

namespace opossum {
  CalibrationTableWrapper::CalibrationTableWrapper(const std::shared_ptr<Table> table,
                                                   const std::vector<ColumnDataDistribution> column_data_distribution_collection)
                                                   : table(std::move(table)), column_data_distribution_collection(std::move(column_data_distribution_collection)){
    assert(table->column_count() == column_data_distribution_collection.size());
  }

  ColumnDataDistribution CalibrationTableWrapper::get_column_data_distribution(ColumnID id) const {
    return column_data_distribution_collection[id];
  }

  std::shared_ptr<Table> CalibrationTableWrapper::getTable() const {
    return table;
  }
}