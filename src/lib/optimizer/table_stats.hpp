#pragma once

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/column_stats.hpp"

namespace opossum {

class Table;

class TableStats {
  friend class Statistics;

 private:
  struct private_key {  // passkey pattern for pseudo-private constructor
    private_key() {}
  };

 public:
  explicit TableStats(const std::weak_ptr<Table> table);
  TableStats(const private_key &, std::weak_ptr<Table> table, double row_count,
             std::map<std::string, std::shared_ptr<ColumnStats>> column_stats);
  double row_count();
  std::shared_ptr<ColumnStats> get_column_stats(const std::string &column_name);

 private:
  std::shared_ptr<TableStats> shared_clone(double row_count);
  const std::weak_ptr<Table> _table;
  double _row_count;
  std::map<std::string, std::shared_ptr<ColumnStats>> _column_stats;
};

}  // namespace opossum
