#pragma once

#include "storage/table.hpp"

namespace opossum {

class CalibrationTableWrapper {
 /*
 * Wraps a table and holds in addition the information about the data distribution
 * Intended for communication from the TableGenerator to the LQPGenerator.
 */
 public:
  CalibrationTableWrapper(
    const std::shared_ptr<Table> table,
    const std::string& table_name,
    const std::vector<ColumnDataDistribution> column_data_distribution_collection);

  const ColumnDataDistribution get_column_data_distribution(ColumnID id) const;

  const std::shared_ptr<Table> get_table() const;

  const std::string &get_name() const;

  void export_table_meta_data(const std::string& filepath) const;

private:
  const std::shared_ptr<Table> _table;
  const std::string _name;
  const std::vector<ColumnDataDistribution> _column_data_distribution_collection;
};
}  // namespace opossum
