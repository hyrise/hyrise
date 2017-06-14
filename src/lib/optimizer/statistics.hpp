#pragma once

#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/table_stats.hpp"
#include "storage/table.hpp"

namespace opossum {

class Statistics {
 public:
  static std::shared_ptr<TableStats> predicate_stats(const std::shared_ptr<TableStats> table_stats,
                                                     const std::string &column_name, const std::string &op,
                                                     const AllParameterVariant value,
                                                     const optional<AllTypeVariant> value2 = nullopt);

 private:
  static size_t get_distinct_count(const std::shared_ptr<Table> table, const std::string &column_name);
};

}  // namespace opossum
