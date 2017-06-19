#include "column_statistics.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

namespace opossum {

ColumnStatistics::ColumnStatistics(const std::weak_ptr<Table> table, const std::string &column_name)
    : _table(table), _column_name(column_name) {}
ColumnStatistics::ColumnStatistics(size_t distinct_count, AllTypeVariant min, AllTypeVariant max,
                                   const std::string &column_name)
    : _column_name(column_name), _distinct_count(distinct_count), _min(min), _max(max) {}
size_t ColumnStatistics::get_distinct_count() { return _distinct_count.value_or(update_distinct_count()); }
AllTypeVariant ColumnStatistics::get_min() { return 0; }
AllTypeVariant ColumnStatistics::get_max() { return 0; }

size_t ColumnStatistics::update_distinct_count() {
  auto shared_table = std::shared_ptr<Table>(_table);
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<std::pair<std::string, AggregateFunction>>{},
                                               std::vector<std::string>{_column_name});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
  return *_distinct_count;
}

}  // namespace opossum
