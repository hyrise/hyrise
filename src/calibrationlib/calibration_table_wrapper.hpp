#pragma once

#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"

namespace opossum {

class CalibrationTableWrapper {
  /*
 * Wraps a table and holds the information about the data distribution
 * Intended for communication from the CalibrationTableGenerator to the CalibrationLQPGenerator.
 */
 public:
  CalibrationTableWrapper(const std::shared_ptr<Table> table, const std::string& table_name,
                          const std::vector<ColumnDataDistribution> column_data_distribution_collection);

  CalibrationTableWrapper(const std::shared_ptr<Table> table, const std::string& table_name);

  const ColumnDataDistribution get_column_data_distribution(ColumnID id) const;

  const std::shared_ptr<Table> get_table() const;

  const std::string& get_name() const;

 private:
  const std::shared_ptr<Table> _table;
  const std::string _name;
  const std::vector<ColumnDataDistribution> _column_data_distribution_collection;
};
}  // namespace opossum
