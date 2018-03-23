#include "table_statistics.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <optional>
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
    : _table(table),
      _table_type(table->type()),
      _row_count(table->row_count()),
      _column_statistics(table->column_count()) {}

TableStatistics::TableStatistics(const TableType table_type, float row_count,
                                 const std::vector<std::shared_ptr<BaseColumnStatistics>>& column_statistics)
    : _table_type(table_type), _row_count(row_count), _column_statistics(column_statistics) {}

TableType TableStatistics::table_type() const { return _table_type; }

float TableStatistics::row_count() const { return _row_count; }

uint64_t TableStatistics::approx_valid_row_count() const { return row_count() - _approx_invalid_row_count; }

const std::vector<std::shared_ptr<BaseColumnStatistics>>& TableStatistics::column_statistics() const {
  // Lazily initialize column statistics
  _create_all_column_statistics();
  return _column_statistics;
}

std::shared_ptr<TableStatistics> TableStatistics::predicate_statistics(const ColumnID column_id,
                                                                       const PredicateCondition predicate_condition,
                                                                       const AllParameterVariant& value,
                                                                       const std::optional<AllTypeVariant>& value2) {
  auto _row_count = row_count();
  if (_row_count == 0) {
    return shared_from_this();
  }

  if (predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) {
    // simple heuristic:
    auto clone = std::make_shared<TableStatistics>(*this);
    auto selectivity = DEFAULT_LIKE_SELECTIVITY;
    if (predicate_condition == PredicateCondition::NotLike) selectivity = 1.0 - selectivity;
    clone->_row_count = _row_count * selectivity;
    return clone;
  }

  auto old_column_statistics = _get_or_generate_column_statistics(column_id);

  // create copy of this as this should not be adapted for current table scan
  auto clone = std::make_shared<TableStatistics>(*this);
  ColumnSelectivityResult column_statistics_container;

  // delegate prediction to corresponding column statistics
  if (value.type() == typeid(ColumnID)) {
    const ColumnID value_column_id = boost::get<ColumnID>(value);
    auto old_right_column_stats = _get_or_generate_column_statistics(value_column_id);

    auto two_column_statistics_container = old_column_statistics->estimate_selectivity_for_two_column_predicate(
        predicate_condition, old_right_column_stats, value2);

    clone->_column_statistics[value_column_id] = two_column_statistics_container.second_column_statistics;
    column_statistics_container = two_column_statistics_container;

  } else if (value.type() == typeid(AllTypeVariant)) {
    auto casted_value = get<AllTypeVariant>(value);

    column_statistics_container =
        old_column_statistics->estimate_selectivity_for_predicate(predicate_condition, casted_value, value2);

  } else {
    DebugAssert(value.type() == typeid(ValuePlaceholder),
                "AllParameterVariant type is not implemented in statistics component.");
    auto casted_value = boost::get<ValuePlaceholder>(value);

    column_statistics_container =
        old_column_statistics->estimate_selectivity_for_predicate(predicate_condition, casted_value, value2);
  }

  clone->_table_type = TableType::References;

  clone->_column_statistics[column_id] = column_statistics_container.column_statistics;

  clone->_row_count *= column_statistics_container.selectivity;

  return clone;
}

std::shared_ptr<TableStatistics> TableStatistics::generate_cross_join_statistics(
    const std::shared_ptr<TableStatistics>& right_table_stats) {
  // create all not yet created column statistics as there is no mapping in join table statistics from table to columns
  // A join result can consist of columns of two different tables. Therefore, the reference to the table cannot be
  // stored within the table statistics but instead in the column statistics.
  _create_all_column_statistics();
  right_table_stats->_create_all_column_statistics();

  // create copy of this as this should not be adapted for current join
  auto join_table_stats = std::make_shared<TableStatistics>(*this);
  join_table_stats->_table_type = TableType::References;

  // make space in output for column statistics of right right table and copy them to output
  join_table_stats->_column_statistics.resize(_column_statistics.size() + right_table_stats->_column_statistics.size());
  auto col_stats_right_begin = join_table_stats->_column_statistics.begin() + _column_statistics.size();
  std::copy(right_table_stats->_column_statistics.begin(), right_table_stats->_column_statistics.end(),
            col_stats_right_begin);

  // all columns are added, table pointer is deleted for output statistics
  join_table_stats->_reset_table_ptr();

  // calculate output size for cross joins
  join_table_stats->_row_count *= right_table_stats->_row_count;
  return join_table_stats;
}

std::shared_ptr<TableStatistics> TableStatistics::generate_predicated_join_statistics(
    const std::shared_ptr<TableStatistics>& right_table_stats, const JoinMode mode, const ColumnIDPair column_ids,
    const PredicateCondition predicate_condition) {
  DebugAssert(mode != JoinMode::Cross, "Use function generate_cross_join_statistics for cross joins.");
  DebugAssert(mode != JoinMode::Natural, "Natural joins are not supported by statistics component.");

  /**
   * The approach to calculate the join table statistics is to split the join into a cross join followed by a predicate.
   *
   * This approach allows to reuse the code to copy the column statistics to the join output statistics from the cross
   * join function. The selectivity of the join predicate can then be calculated by the two column predicate function
   * within column statistics. The calculated selectivity can then be applied to the cross join result.
   *
   * For left/right/outer joins the occurring null values will result in changed null value ratios in partial/all column
   * statistics of the join statistics.
   * To calculate the changed ratios, the new total number of null values in a column as well as the join table row count
   * are necessary. Remember that statistics component assumes NULL != NULL semantics.
   *
   * The calculation of null values is shown by following SQL query: SELECT * FROM TABLE_1 OUTER JOIN TABLE_2 ON a = c
   *
   *   TABLE_1         TABLE_2          CROSS_JOIN_TABLE              INNER / LEFT  / RIGHT / OUTER JOIN
   *
   *    a    | b        c    | d         a    | b    | c    | d        a    | b    | c    | d
   *   -------------   --------------   --------------------------    --------------------------
   *    1    | NULL     1    | 30        1    | NULL | 1    | 30       1    | NULL | 1    | 30
   *    2    | 10       NULL | 40        2    | 10   | 1    | 30
   *    NULL | 20                        NULL | 20   | 1    | 30      INNER +0 extra rows
   *                                     1    | NULL | NULL | 40      LEFT  +2 extra rows
   *                                     2    | 10   | NULL | 40      RIGHT +1 extra rows
   *                                     NULL | 20   | NULL | 40      OUTER +3 extra rows (the ones from LEFT & RIGHT)
   *
   * First, the cross join row count is calculated: 3 * 2 = 6
   * Then, the selectivity for non-null values is calculated: 1/2 (50% of the non-null values from column a match the
   * value 1 from column c)
   * Next, the predicate selectivity is calculated: non-null predicate selectivity * left-non-null * right-non-null
   * = 1/2 * 2/3 * 1/2 = 1/6
   * For an inner join, the row count would then be: row count * predicate selectivity = 6 * 1/6 = 1
   *
   * The selectivity calculation call also returns the new column statistics for columns a and c. Both are identical and
   * have a min, max value of 1, distinct count of 1 and a non-null value ratio of 1.
   * These new column statistics replace the old corresponding column statistics in the output table statistics, if the
   * join mode does not specify to keep all values of a column.
   * E.g. the new left column statistics replaces its previous statistics, if join mode is self, inner or right.
   * Vice versa the new right column statistics replaces its previous statistic, if join mode is self, inner or left.
   *
   * For a full outer join, the null values added to columns c and d are the number of null values of column a (= 1)
   * plus the number of non-null values of column a not selected by the predicate (= 1 (value 2 in row 2)).
   * So in total 1 + 1 = 2 null values are added to columns c and d. Column c had already a null value before and,
   * therefore, has now 1 + 2 = 3 null values. Column d did not have null values and now has 0 + 2 = 2 null values.
   * The same calculations also needs to be done for the null value numbers in columns a and b. Since all non-null
   * values in column c are selected by the predicate only the null value number of column c needs to be added to
   * columns a and b: 0 + 1 = 1
   * Columns a and b both have 1 null value before the join and, therefore, both have 1 + 1 = 2 null values after the
   * join.
   *
   * The row count of the join result is calculated by taking the row count of the inner join (= 1) and adding the null
   * value numbers which were added to the columns from the left table (= 1) and the right table (= 2). This results in
   * the row count for the outer join of 1 + 1 + 2 = 4.
   *
   * For a left outer join, the join table would just miss the 4th row. For this join, the number of null values to add
   * to the right columns would still be 1 + 1 = 2. However, no null values are added to the the left columns.
   * This results in a join result row count of 1 + 2 = 3.
   */

  // For self joins right_table_stats should be this.
  if (mode == JoinMode::Self) {
    DebugAssert(shared_from_this() == right_table_stats,
                "Self joins should pass the same table as right_table_stats again.");
  }

  // copy column statistics and calculate cross join row count
  auto join_table_stats = generate_cross_join_statistics(right_table_stats);

  // retrieve the two column statistics which are used by the join predicate
  auto& left_col_stats = _column_statistics[column_ids.first];
  auto& right_col_stats = right_table_stats->_column_statistics[column_ids.second];

  auto stats_container =
      left_col_stats->estimate_selectivity_for_two_column_predicate(predicate_condition, right_col_stats);

  // apply predicate selectivity to cross join
  join_table_stats->_row_count *= stats_container.selectivity;

  ColumnID new_right_column_id{static_cast<ColumnID::base_type>(_column_statistics.size() + column_ids.second)};

  // calculate how many null values need to be added to columns from the left table for right/outer joins
  auto left_null_value_no = _calculate_added_null_values_for_outer_join(
      right_table_stats->row_count(), right_col_stats, stats_container.second_column_statistics->distinct_count());
  // calculate how many null values need to be added to columns from the right table for left/outer joins
  auto right_null_value_no = _calculate_added_null_values_for_outer_join(
      row_count(), left_col_stats, stats_container.column_statistics->distinct_count());

  // prepare two _adjust_null_value_ratio_for_outer_join calls, executed in the switch statement below

  // a) add null values to columns from the right table for left outer join
  auto apply_left_outer_join = [&]() {
    _adjust_null_value_ratio_for_outer_join(join_table_stats->_column_statistics.begin() + _column_statistics.size(),
                                            join_table_stats->_column_statistics.end(), right_table_stats->row_count(),
                                            right_null_value_no, join_table_stats->row_count());
  };
  // b) add null values to columns from the left table for right outer
  auto apply_right_outer_join = [&]() {
    _adjust_null_value_ratio_for_outer_join(join_table_stats->_column_statistics.begin(),
                                            join_table_stats->_column_statistics.begin() + _column_statistics.size(),
                                            row_count(), left_null_value_no, join_table_stats->row_count());
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

void TableStatistics::increment_invalid_row_count(uint64_t count) { _approx_invalid_row_count += count; }

std::shared_ptr<BaseColumnStatistics> TableStatistics::_get_or_generate_column_statistics(
    const ColumnID column_id) const {
  if (column_id < _column_statistics.size() && _column_statistics[column_id]) {
    return _column_statistics[column_id];
  }

  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");
  auto column_type = table->column_data_type(column_id);
  auto column_statistics =
      make_shared_by_data_type<BaseColumnStatistics, ColumnStatistics>(column_type, column_id, _table);
  _column_statistics[column_id] = column_statistics;
  return _column_statistics[column_id];
}

void TableStatistics::_create_all_column_statistics() const {
  for (ColumnID column_id{0}; column_id < _column_statistics.size(); ++column_id) {
    _get_or_generate_column_statistics(column_id);
  }
}

void TableStatistics::_reset_table_ptr() {
  for (ColumnID column_id{0}; column_id < _column_statistics.size(); ++column_id) {
    DebugAssert(_column_statistics[column_id], "All column statistics of table statistics have to exist.");
  }
  _table.reset();
}

float TableStatistics::_calculate_added_null_values_for_outer_join(
    const float row_count, const std::shared_ptr<BaseColumnStatistics> col_stats,
    const float predicate_column_distinct_count) const {
  float null_value_no = col_stats->null_value_ratio() * row_count;
  if (col_stats->distinct_count() != 0.f) {
    null_value_no += (1.f - predicate_column_distinct_count / col_stats->distinct_count()) * row_count;
  }
  return null_value_no;
}

void TableStatistics::_adjust_null_value_ratio_for_outer_join(
    const std::vector<std::shared_ptr<BaseColumnStatistics>>::iterator col_begin,
    const std::vector<std::shared_ptr<BaseColumnStatistics>>::iterator col_end, const float row_count,
    const float null_value_no, const float new_row_count) {
  if (null_value_no == 0) {
    return;
  }
  // adjust null value ratios in columns from the right table
  for (auto col_itr = col_begin; col_itr != col_end; ++col_itr) {
    // columns need to be copied before changed, somebody else could use it
    *col_itr = (*col_itr)->clone();
    float column_null_value_no = (*col_itr)->null_value_ratio() * row_count;
    float right_null_value_ratio = (column_null_value_no + null_value_no) / new_row_count;
    (*col_itr)->set_null_value_ratio(right_null_value_ratio);
  }
}

std::ostream& operator<<(std::ostream& os, TableStatistics& obj) {
  os << "Table Stats " << std::endl;
  os << " row count: " << obj._row_count;
  for (const auto& statistics : obj._column_statistics) {
    if (statistics) os << std::endl << " " << *statistics;
  }
  return os;
}

}  // namespace opossum
