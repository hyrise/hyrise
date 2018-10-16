#include "table_statistics.hpp"

#include <sstream>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "base_column_statistics.hpp"

namespace opossum {

TableStatistics::TableStatistics(const TableType table_type, const float row_count,
                                 const std::vector<std::shared_ptr<const BaseColumnStatistics>>& column_statistics)
    : _table_type(table_type), _row_count(row_count), _column_statistics(column_statistics) {}

TableType TableStatistics::table_type() const { return _table_type; }

float TableStatistics::row_count() const { return _row_count; }

uint64_t TableStatistics::approx_valid_row_count() const { return row_count() - _approx_invalid_row_count; }

const std::vector<std::shared_ptr<const BaseColumnStatistics>>& TableStatistics::column_statistics() const {
  return _column_statistics;
}

TableStatistics TableStatistics::estimate_predicate(const ColumnID column_id,
                                                    const PredicateCondition predicate_condition,
                                                    const AllParameterVariant& value,
                                                    const std::optional<AllParameterVariant>& value2) const {
  // Early out, the code below would fail for _row_count == 0
  if (_row_count == 0) return {*this};

  /**
   * This function mostly dispatches the matching ColumnStatistics::estimate_*() function
   */

  // Estimate "a BETWEEN 5 and 6" by combining "a >= 5" with "a <= 6"
  if (predicate_condition == PredicateCondition::Between) {
    DebugAssert(value2, "Expected second value to be passed in for BETWEEN");
    auto table_statistics = estimate_predicate(column_id, PredicateCondition::GreaterThanEquals, value);
    return table_statistics.estimate_predicate(column_id, PredicateCondition::LessThanEquals, *value2);
  }

  // TODO(anybody) we don't do (Not)Like estimations yet, thus resort to magic numbers
  if (predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) {
    const auto selectivity =
        predicate_condition == PredicateCondition::Like ? DEFAULT_LIKE_SELECTIVITY : 1.0f - DEFAULT_LIKE_SELECTIVITY;
    return {TableType::References, _row_count * selectivity, _column_statistics};
  }

  // Create copies to modify below and insert into result
  auto predicated_row_count = _row_count;
  auto predicated_column_statistics = _column_statistics;

  const auto left_operand_column_statistics = _column_statistics[column_id];

  if (predicate_condition == PredicateCondition::IsNotNull) {
    predicated_column_statistics[column_id] = left_operand_column_statistics->without_null_values();
    predicated_row_count *= 1.0 - left_operand_column_statistics->non_null_value_ratio();
  } else if (predicate_condition == PredicateCondition::IsNull) {
    predicated_column_statistics[column_id] = left_operand_column_statistics->only_null_values();
    predicated_row_count *= left_operand_column_statistics->non_null_value_ratio();
  } else if (is_column_id(value)) {
    const auto column_id_of_value = boost::get<ColumnID>(value);
    const auto estimation = left_operand_column_statistics->estimate_predicate_with_column(
        predicate_condition, *_column_statistics[column_id_of_value]);

    predicated_column_statistics[column_id] = estimation.left_column_statistics;
    predicated_column_statistics[column_id_of_value] = estimation.right_column_statistics;
    predicated_row_count *= estimation.selectivity;
  } else if (is_variant(value)) {
    const auto variant_value = boost::get<AllTypeVariant>(value);

    const auto estimate =
        left_operand_column_statistics->estimate_predicate_with_value(predicate_condition, variant_value);

    predicated_column_statistics[column_id] = estimate.column_statistics;
    predicated_row_count *= estimate.selectivity;
  } else {
    Assert(is_parameter_id(value), "AllParameterVariant type is not implemented in statistics component.");
    const auto estimate =
        left_operand_column_statistics->estimate_predicate_with_value_placeholder(predicate_condition);

    predicated_column_statistics[column_id] = estimate.column_statistics;
    predicated_row_count *= estimate.selectivity;
  }

  return {TableType::References, predicated_row_count, predicated_column_statistics};
}

TableStatistics TableStatistics::estimate_cross_join(const TableStatistics& right_table_statistics) const {
  /**
   * Cross Join Estimation is simple:
   *    cross_joined_row_count:             The product of both input row counts
   *    cross_joined_column_statistics:     The concatenated list of column statistics
   */

  // Create copies to manipulate and to use for result
  auto cross_joined_table_statistics = *this;

  auto cross_joined_column_statistics = _column_statistics;
  cross_joined_column_statistics.reserve(_column_statistics.size() + right_table_statistics._column_statistics.size());

  for (const auto& column_statistics : right_table_statistics._column_statistics) {
    cross_joined_column_statistics.emplace_back(column_statistics);
  }

  auto cross_joined_row_count = _row_count * right_table_statistics._row_count;

  return {TableType::References, cross_joined_row_count, cross_joined_column_statistics};
}

TableStatistics TableStatistics::estimate_predicated_join(const TableStatistics& right_table_statistics,
                                                          const JoinMode mode, const ColumnIDPair column_ids,
                                                          const PredicateCondition predicate_condition) const {
  Assert(mode != JoinMode::Cross, "Use function estimate_cross_join for cross joins.");

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
  // copy column statistics and calculate cross join row count
  auto join_table_stats = estimate_cross_join(right_table_statistics);

  // retrieve the two column statistics which are used by the join predicate
  auto& left_column_stats = _column_statistics[column_ids.first];
  auto& right_column_stats = right_table_statistics._column_statistics[column_ids.second];

  auto stats_container = left_column_stats->estimate_predicate_with_column(predicate_condition, *right_column_stats);

  // apply predicate selectivity to cross join
  join_table_stats._row_count *= stats_container.selectivity;

  ColumnID new_right_column_id{static_cast<ColumnID::base_type>(_column_statistics.size() + column_ids.second)};

  auto calculate_added_null_values_for_outer_join = [&](const float row_count,
                                                        const std::shared_ptr<const BaseColumnStatistics> column_stats,
                                                        const float predicate_column_distinct_count) {
    float null_value_no = column_stats->null_value_ratio() * row_count;
    if (column_stats->distinct_count() != 0.f) {
      null_value_no += (1.f - predicate_column_distinct_count / column_stats->distinct_count()) * row_count;
    }
    return null_value_no;
  };

  auto adjust_null_value_ratio_for_outer_join =
      [&](const std::vector<std::shared_ptr<const BaseColumnStatistics>>::iterator column_begin,
          const std::vector<std::shared_ptr<const BaseColumnStatistics>>::iterator column_end, const float row_count,
          const float null_value_no, const float new_row_count) {
        if (null_value_no == 0) {
          return;
        }
        // adjust null value ratios in columns from the right table
        for (auto column_itr = column_begin; column_itr != column_end; ++column_itr) {
          // columns need to be copied before changed, somebody else could use it
          *column_itr = (*column_itr)->clone();
          float column_null_value_no = (*column_itr)->null_value_ratio() * row_count;
          float right_null_value_ratio = (column_null_value_no + null_value_no) / new_row_count;

          // We just created these column statistics and are therefore qualified to modify them
          std::const_pointer_cast<BaseColumnStatistics>(*column_itr)->set_null_value_ratio(right_null_value_ratio);
        }
      };

  // calculate how many null values need to be added to columns from the left table for right/outer joins
  auto left_null_value_no =
      calculate_added_null_values_for_outer_join(right_table_statistics.row_count(), right_column_stats,
                                                 stats_container.right_column_statistics->distinct_count());
  // calculate how many null values need to be added to columns from the right table for left/outer joins
  auto right_null_value_no = calculate_added_null_values_for_outer_join(
      row_count(), left_column_stats, stats_container.left_column_statistics->distinct_count());

  // prepare two _adjust_null_value_ratio_for_outer_join calls, executed in the switch statement below

  // a) add null values to columns from the right table for left outer join
  auto apply_left_outer_join = [&]() {
    adjust_null_value_ratio_for_outer_join(join_table_stats._column_statistics.begin() + _column_statistics.size(),
                                           join_table_stats._column_statistics.end(),
                                           right_table_statistics.row_count(), right_null_value_no,
                                           join_table_stats.row_count());
  };
  // b) add null values to columns from the left table for right outer
  auto apply_right_outer_join = [&]() {
    adjust_null_value_ratio_for_outer_join(join_table_stats._column_statistics.begin(),
                                           join_table_stats._column_statistics.begin() + _column_statistics.size(),
                                           row_count(), left_null_value_no, join_table_stats.row_count());
  };

  switch (mode) {
    case JoinMode::Semi:
      join_table_stats._column_statistics[column_ids.first] = stats_container.left_column_statistics;
      // remove column statistics from right table
      join_table_stats._column_statistics.resize(_column_statistics.size());

      // Simple heuristic: we assume that three quarters of the elements in the smaller relation
      // (we are upper-bound by number of non-null values in both relations) will match.
      join_table_stats._row_count =
          0.75 * std::min(row_count() * stats_container.left_column_statistics->non_null_value_ratio(),
                          right_table_statistics.row_count() *
                              stats_container.right_column_statistics->non_null_value_ratio());
      break;
    case JoinMode::Anti:
      join_table_stats._column_statistics[column_ids.first] = stats_container.left_column_statistics;

      // For anti join, we assume that all values qualify when we have a small "other" relations.
      // In case we have a large right table, we take the maximum of the left table and a tenth of the right.
      join_table_stats._row_count =
          std::max(row_count(), right_table_statistics.row_count() *
                                    stats_container.right_column_statistics->non_null_value_ratio() / 10);
      break;
    case JoinMode::Inner: {
      join_table_stats._column_statistics[column_ids.first] = stats_container.left_column_statistics;
      join_table_stats._column_statistics[new_right_column_id] = stats_container.right_column_statistics;
      break;
    }
    case JoinMode::Left: {
      join_table_stats._column_statistics[new_right_column_id] = stats_container.right_column_statistics;
      join_table_stats._row_count += right_null_value_no;
      apply_left_outer_join();
      break;
    }
    case JoinMode::Right: {
      join_table_stats._column_statistics[column_ids.first] = stats_container.left_column_statistics;
      join_table_stats._row_count += left_null_value_no;
      apply_right_outer_join();
      break;
    }
    case JoinMode::Outer: {
      join_table_stats._row_count += right_null_value_no;
      join_table_stats._row_count += left_null_value_no;
      apply_left_outer_join();
      apply_right_outer_join();
      break;
    }
    default: { Fail("Join mode not implemented."); }
  }

  return join_table_stats;
}

void TableStatistics::increase_invalid_row_count(uint64_t count) { _approx_invalid_row_count += count; }

TableStatistics TableStatistics::estimate_disjunction(const TableStatistics& right_table_statistics) const {
  // TODO(anybody) this is just a dummy implementation
  return {TableType::References, row_count() + right_table_statistics.row_count() * DEFAULT_DISJUNCTION_SELECTIVITY,
          column_statistics()};
}

std::string TableStatistics::description() const {
  std::stringstream stream;

  stream << "Table Stats " << std::endl;
  stream << " row count: " << _row_count;
  for (const auto& statistics : _column_statistics) {
    stream << std::endl << " " << statistics->description();
  }
  return stream.str();
}

}  // namespace opossum
