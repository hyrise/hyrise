#include "column_stats.hpp"

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

ColumnStats::ColumnStats(const std::weak_ptr<Table> table, const std::string &column_name)
    : _table(table), _column_name(column_name) {}
size_t ColumnStats::get_distinct_count() { return _distinct_count.value_or(update_distinct_count()); }
AllTypeVariant ColumnStats::get_min() { return 0; }
AllTypeVariant ColumnStats::get_max() { return 0; }

size_t ColumnStats::update_distinct_count() {
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
