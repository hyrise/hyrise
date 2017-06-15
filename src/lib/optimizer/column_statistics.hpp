#pragma once

#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

class Aggregate;
class Table;
class TableWrapper;

class ColumnStatistics {
 public:
  ColumnStatistics(const std::weak_ptr<Table> table, const std::string &column_name);
  size_t get_distinct_count();
  AllTypeVariant get_min();
  AllTypeVariant get_max();

 private:
  size_t update_distinct_count();

  const std::weak_ptr<Table> _table;
  const std::string &_column_name;
  optional<size_t> _distinct_count;
  optional<AllTypeVariant> _min;
  optional<AllTypeVariant> _max;
};

}  // namespace opossum
