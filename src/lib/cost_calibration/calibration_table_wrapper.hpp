#pragma once
#include "storage/table.hpp"

namespace opossum {

class CalibrationTableWrapper {
 public:
  CalibrationTableWrapper(
    std::shared_ptr<Table> table,
    std::vector<ColumnDataDistribution> column_data_distribution_collection);

  [[nodiscard]] ColumnDataDistribution get_column_data_distribution(ColumnID id) const;

  [[nodiscard]] std::shared_ptr<Table> getTable() const;

 private:
  const std::shared_ptr<Table> table;
  const std::vector<ColumnDataDistribution> column_data_distribution_collection;
};
}  // namespace opossum
