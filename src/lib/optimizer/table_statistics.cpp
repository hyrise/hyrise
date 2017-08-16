#include "table_statistics.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "optimizer/base_column_statistics.hpp"
#include "optimizer/column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStatistics::TableStatistics(const std::shared_ptr<Table> table)
    : _table(table), _row_count(table->row_count()), _column_statistics(_row_count) {}

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

void TableStatistics::create_all_column_statistics() {
  // table pointer is deleted, if all column statistics are already created
  auto table_nullptr = std::weak_ptr<Table>();
  // check if _table is null_ptr
  if (!table_nullptr.owner_before(_table)) {
    return;
  }
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");

  for (ColumnID column_id{0}; column_id < _column_statistics.size(); ++column_id) {
    if (!_column_statistics[column_id]) {
      auto column_type = table->column_type(column_id);
      _column_statistics[column_id] =
          make_shared_by_column_type<BaseColumnStatistics, ColumnStatistics>(column_type, column_id, _table);
    }
  }
  _table.reset();
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

  auto old_column_statistics = column_statistics(column_id);

  // create copy of this as this should not be adapted for current table scan
  auto clone = std::make_shared<TableStatistics>(*this);
  ColumnSelectivityResult column_statistics_container{1, nullptr};

  // delegate prediction to corresponding column statistics
  if (value.type() == typeid(ColumnID)) {
    const ColumnID value_column_id = get<ColumnID>(value);
    auto old_right_column_stats = column_statistics(value_column_id);

    auto two_column_statistics_container =
        old_column_statistics->estimate_selectivity_for_two_column_predicate(scan_type, old_right_column_stats, value2);

    if (two_column_statistics_container.second_column_statistics != nullptr) {
      clone->_column_statistics[value_column_id] = two_column_statistics_container.second_column_statistics;
    }
    column_statistics_container = two_column_statistics_container;

  } else if (value.type() == typeid(AllTypeVariant)) {
    auto casted_value = get<AllTypeVariant>(value);

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

std::shared_ptr<TableStatistics> TableStatistics::join_statistics(
    const std::shared_ptr<TableStatistics> &right_table_statistics,
    const optional<std::pair<ColumnID, ColumnID>> column_ids, const ScanType scan_type, const JoinMode mode) {
  // create all not yet created column statistics as there is no mapping in table statistics from table to columns
  create_all_column_statistics();
  right_table_statistics->create_all_column_statistics();

  // create copy of this as this should not be adapted for current join
  auto clone = std::make_shared<TableStatistics>(*this);
  auto col_stats_end = clone->_column_statistics.end();

  auto right_stats = (mode == JoinMode::Self) ? clone : right_table_statistics;

  clone->_column_statistics.resize(clone->_column_statistics.size() + right_stats->_column_statistics.size());
  std::copy(right_table_statistics->_column_statistics.begin(), right_stats->_column_statistics.end(), col_stats_end);

  clone->_row_count *= right_table_statistics->_row_count;
  if (mode == JoinMode::Cross) {
    return clone;
  }

  DebugAssert(static_cast<bool>(column_ids), "Column ids required for all non cross-join joins.");

  auto left_col_stats = _column_statistics[column_ids->first];
  auto right_col_stats = right_stats->_column_statistics[column_ids->second];

  auto stats_container = left_col_stats->estimate_selectivity_for_two_column_predicate(scan_type, right_col_stats);
  if (!stats_container.column_statistics) {
    stats_container.column_statistics = left_col_stats;
  }
  if (!stats_container.second_column_statistics) {
    stats_container.second_column_statistics = right_col_stats;
  }

  clone->_row_count *= stats_container.selectivity;

  ColumnID new_right_column_id{_column_statistics.size() + column_ids->second};

  switch (mode) {
    case JoinMode::Self:
    case JoinMode::Inner: {
      clone->_column_statistics[column_ids->first] = stats_container.column_statistics;
      clone->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
    }
    case JoinMode::Left: {
      clone->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      float null_ratio = 1.f;
      if (left_col_stats->distinct_count() != 0.f) {
        null_ratio -= stats_container.column_statistics->distinct_count() / left_col_stats->distinct_count();
      }
      clone->_row_count += null_ratio * _row_count;
    }
    case JoinMode::Right: {
      clone->_column_statistics[column_ids->first] = stats_container.column_statistics;
      float null_ratio = 1.f;
      if (right_col_stats->distinct_count() != 0.f) {
        null_ratio -= stats_container.column_statistics->distinct_count() / right_col_stats->distinct_count();
      }
      clone->_row_count += null_ratio * right_stats->_row_count;
    }
    case JoinMode::Outer: {
      float null_ratio = 1.f;
      if (left_col_stats->distinct_count() != 0.f) {
        null_ratio -= stats_container.column_statistics->distinct_count() / left_col_stats->distinct_count();
      }
      clone->_row_count += null_ratio * _row_count;
      null_ratio = 1.f;
      if (right_col_stats->distinct_count() != 0.f) {
        null_ratio -= stats_container.column_statistics->distinct_count() / right_col_stats->distinct_count();
      }
      clone->_row_count += null_ratio * right_stats->_row_count;
    }
    case JoinMode::Natural: {
      Fail("Natural join not possible as column ids are used.");
    }
    default: { Fail("Join mode not implemented."); }
  }

  return clone;
}

}  // namespace opossum
