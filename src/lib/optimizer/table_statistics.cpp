#include "table_statistics.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "optimizer/base_column_statistics.hpp"
#include "optimizer/column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStatistics::TableStatistics(const std::shared_ptr<Table> table)
    : _table(table), _row_count(table->row_count()), _column_statistics(table->col_count()) {}

float TableStatistics::row_count() const { return _row_count; }

std::shared_ptr<BaseColumnStatistics> TableStatistics::column_statistics(const ColumnID column_id) {
  if (_column_statistics[column_id]) {
    return _column_statistics[column_id];
  }

  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");
  auto column_type = table->column_type(column_id);
  auto column_statistics =
      make_shared_by_column_type<BaseColumnStatistics, ColumnStatistics>(column_type, column_id, _table);
  _column_statistics[column_id] = column_statistics;
  return _column_statistics[column_id];
}

std::shared_ptr<TableStatistics> TableStatistics::predicate_statistics(const ColumnID column_id,
                                                                       const ScanType scan_type,
                                                                       const AllParameterVariant &value,
                                                                       const optional<AllTypeVariant> &value2) {
  auto _row_count = row_count();
  if (_row_count == 0) {
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count;
    return clone;
  }

  if (scan_type == ScanType::OpLike) {
    // simple heuristic:
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count * DEFAULT_LIKE_SELECTIVITY;
    return clone;
  }

  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");

  auto old_column_statistics = column_statistics(column_id);

  // create copy of this as this should not be adapted for current table scan
  auto clone = std::make_shared<TableStatistics>(*this);
  ColumnSelectivityResult column_statistics_container{1, nullptr};

  // delegate prediction to corresponding column statistics
  if (value.type() == typeid(ColumnID)) {
    const ColumnID value_column_id = boost::get<ColumnID>(value);
    auto old_right_column_stats = column_statistics(value_column_id);

    auto two_column_statistics_container =
        old_column_statistics->estimate_selectivity_for_two_column_predicate(scan_type, old_right_column_stats, value2);

    if (two_column_statistics_container.second_column_statistics != nullptr) {
      clone->_column_statistics[value_column_id] = two_column_statistics_container.second_column_statistics;
    }
    column_statistics_container = two_column_statistics_container;

  } else if (value.type() == typeid(AllTypeVariant)) {
    auto casted_value = boost::get<AllTypeVariant>(value);

    column_statistics_container =
        old_column_statistics->estimate_selectivity_for_predicate(scan_type, casted_value, value2);

  } else {
    DebugAssert(value.type() == typeid(ValuePlaceholder),
                "AllParameterVariant type is not implemented in statistics component.");
    auto casted_value = boost::get<ValuePlaceholder>(value);

    column_statistics_container =
        old_column_statistics->estimate_selectivity_for_predicate(scan_type, casted_value, value2);
  }

  if (column_statistics_container.column_statistics != nullptr) {
    clone->_column_statistics[column_id] = column_statistics_container.column_statistics;
  }
  clone->_row_count *= column_statistics_container.selectivity;

  return clone;
}

}  // namespace opossum
