#pragma once

#include <iostream>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

class Aggregate;
class Table;
class TableWrapper;

template <typename T>
std::ostream& operator<<(std::ostream& os, opossum::optional<T>& obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "NA";
  }
}

class ColumnStatistics {
 public:
  ColumnStatistics(const std::weak_ptr<Table> table, const std::string& column_name);
  ColumnStatistics(size_t distinct_count, AllTypeVariant min, AllTypeVariant max, const std::string& column_name);
  size_t get_distinct_count();
  AllTypeVariant get_min();
  AllTypeVariant get_max();
  friend std::ostream& operator<<(std::ostream& os, ColumnStatistics& obj) {
    os << "Col Stats " << obj._column_name << std::endl;
    os << "  dist. " << obj._distinct_count << std::endl;
    os << "  min   " << obj._min << std::endl;
    os << "  max   " << obj._max;
    return os;
  }

 private:
  size_t update_distinct_count();

  const std::weak_ptr<Table> _table;
  const std::string& _column_name;
  optional<size_t> _distinct_count;
  optional<AllTypeVariant> _min;
  optional<AllTypeVariant> _max;
};

}  // namespace opossum
