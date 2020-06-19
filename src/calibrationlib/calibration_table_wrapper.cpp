#include "calibration_table_wrapper.hpp"

#include "storage/abstract_encoded_segment.hpp"

namespace opossum {
CalibrationTableWrapper::CalibrationTableWrapper(
    const std::shared_ptr<Table> table, const std::string& table_name,
    const std::vector<ColumnDataDistribution> column_data_distribution_collection)
    : _table(table), _name(table_name), _column_data_distribution_collection(column_data_distribution_collection) {
  Assert(table->column_count() == column_data_distribution_collection.size(),
         "Number of column data distributions must equal the number of columns.");
}

CalibrationTableWrapper::CalibrationTableWrapper(const std::shared_ptr<Table> table, const std::string& table_name)
    : _table(table), _name(table_name), _column_data_distribution_collection() {}

const std::shared_ptr<Table> CalibrationTableWrapper::get_table() const { return _table; }

const std::string& CalibrationTableWrapper::get_name() const { return _name; }

const ColumnDataDistribution CalibrationTableWrapper::get_column_data_distribution(ColumnID id) const {
  return _column_data_distribution_collection.at(id);
}
}  // namespace opossum
