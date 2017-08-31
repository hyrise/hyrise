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
    const std::shared_ptr<TableStatistics> &right_table_statistics, const JoinMode mode) {
  DebugAssert(mode != JoinMode::Natural, "Natural join not supported by column statistics.");
  DebugAssert(mode == JoinMode::Cross, "Specified JoinMode must also specify column ids and scan type.");

  // create all not yet created column statistics as there is no mapping in table statistics from table to columns
  create_all_column_statistics();
  right_table_statistics->create_all_column_statistics();

  // create copy of this as this should not be adapted for current join
  auto join_table_stats = std::make_shared<TableStatistics>(*this);

  // copy columns of right input to output
  join_table_stats->_column_statistics.resize(_column_statistics.size() +
                                              right_table_statistics->_column_statistics.size());
  auto col_stats_right_begin = join_table_stats->_column_statistics.begin() + _column_statistics.size();
  std::copy(right_table_statistics->_column_statistics.begin(), right_table_statistics->_column_statistics.end(),
            col_stats_right_begin);

  join_table_stats->_row_count *= right_table_statistics->_row_count;
  return join_table_stats;
}

std::shared_ptr<TableStatistics> TableStatistics::join_statistics(
    const std::shared_ptr<TableStatistics> &right_table_statistics, const JoinMode mode,
    const std::pair<ColumnID, ColumnID> column_ids, const ScanType scan_type) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor scan type.");

  auto right_stats = right_table_statistics;
  if (mode == JoinMode::Self) {
    right_stats = shared_from_this();
  }

  auto join_table_stats = join_statistics(right_stats, JoinMode::Cross);

  auto &left_col_stats = _column_statistics[column_ids.first];
  auto &right_col_stats = right_stats->_column_statistics[column_ids.second];

  auto stats_container = left_col_stats->estimate_selectivity_for_two_column_predicate(scan_type, right_col_stats);
  if (!stats_container.column_statistics) {
    stats_container.column_statistics = left_col_stats;
  }
  if (!stats_container.second_column_statistics) {
    stats_container.second_column_statistics = right_col_stats;
  }

  join_table_stats->_row_count *= stats_container.selectivity;

  ColumnID new_right_column_id{static_cast<uint16_t>(_column_statistics.size()) + column_ids.second};

  float left_null_value_no = right_col_stats->null_value_ratio() * right_stats->_row_count;
  if (right_col_stats->distinct_count() != 0.f) {
    left_null_value_no +=
        (1.f - stats_container.second_column_statistics->distinct_count() / right_col_stats->distinct_count()) *
        right_stats->row_count();
  }
  float right_null_value_no = left_col_stats->null_value_ratio() * _row_count;
  if (left_col_stats->distinct_count() != 0.f) {
    right_null_value_no +=
        (1.f - stats_container.column_statistics->distinct_count() / left_col_stats->distinct_count()) * row_count();
  }

  auto apply_left_outer_join = [&]() {
    if (right_null_value_no == 0) {
      return;
    }
    // adjust null value ratios in columns of right side
    for (auto col_itr = join_table_stats->_column_statistics.begin() + _column_statistics.size();
         col_itr != join_table_stats->_column_statistics.end(); ++col_itr) {
      *col_itr = (*col_itr)->clone();
      float column_null_value_no = (*col_itr)->null_value_ratio() * right_stats->_row_count;
      float right_null_value_ratio = (column_null_value_no + right_null_value_no) / join_table_stats->row_count();
      (*col_itr)->set_null_value_ratio(right_null_value_ratio);
    }
  };
  auto apply_right_outer_join = [&]() {
    if (left_null_value_no == 0) {
      return;
    }
    // adjust null value ratios in columns of left side
    for (auto col_itr = join_table_stats->_column_statistics.begin();
         col_itr != join_table_stats->_column_statistics.begin() + _column_statistics.size(); ++col_itr) {
      *col_itr = (*col_itr)->clone();
      float column_null_value_no = (*col_itr)->null_value_ratio() * _row_count;
      float left_null_value_ratio = (column_null_value_no + left_null_value_no) / join_table_stats->row_count();
      (*col_itr)->set_null_value_ratio(left_null_value_ratio);
    }
  };

  switch (mode) {
    case JoinMode::Self:
    case JoinMode::Inner: {
      join_table_stats->_column_statistics[column_ids.first] = stats_container.column_statistics;
      join_table_stats->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      break;
    }
    case JoinMode::Left: {
      join_table_stats->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      join_table_stats->_row_count += right_null_value_no;
      apply_left_outer_join();
      break;
    }
    case JoinMode::Right: {
      join_table_stats->_column_statistics[column_ids.first] = stats_container.column_statistics;
      join_table_stats->_row_count += left_null_value_no;
      apply_right_outer_join();
      break;
    }
    case JoinMode::Outer: {
      join_table_stats->_row_count += right_null_value_no;
      join_table_stats->_row_count += left_null_value_no;
      apply_left_outer_join();
      apply_right_outer_join();
      break;
    }
    default: { Fail("Join mode not implemented."); }
  }

  return join_table_stats;
}

}  // namespace opossum
