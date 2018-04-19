#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "optimizer/base_column_statistics.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/**
 * TableStatistics is the interface to the statistics component.
 *
 * It represents the expected statistics of a table.
 * This can be either a table in the StorageManager, or a result of an operator.
 *
 * TableStatistics can be chained in the same way as operators.
 * The initial table statistics is available via function table_statistics() from the corresponding table.
 *
 * TableStatistics will eventually implement a function for each operator, currently supporting only predicates
 * (via predicate_statistics()) and joins (via join_statistics()) from which the expected row count of the corresponding
 * output table can be accessed.
 *
 * The statistics component assumes a uniform value distribution in columns. If values for predictions are missing
 * (e.g. placeholders in prepared statements), default selectivity values from below are used.
 * The null value support within the statistics component is currently limited. Null value information is stored for
 * every column. This information is used wherever needed (e.g. in predicates) and also updated (e.g. in outer joins).
 * However, this component cannot compute the null value numbers of a column of the corresponding tables. So currently,
 * the statistics component only knows of null values which were introduced through outer joins.
 * The statistics component assumes NULL != NULL semantics.
 *
 * TableStatistics store column statistics as BaseColumnStatistics, which are instances of
 * ColumnStatistics<ColumnType>
 * Public TableStatistics functions pass on the parameters to the corresponding column statistics functions.
 * These compute a new ColumnStatistics<> and the predicted selectivity of an operator.
 *
 * Find more information about table statistics in our wiki:
 * https://github.com/hyrise/hyrise/wiki/potential_statistics
 */
class TableStatistics : public std::enable_shared_from_this<TableStatistics> {
 public:
  /**
   * Creates a table statistics from a table.
   * This should only be done by the storage manager when adding a table to storage manager.
   */
  explicit TableStatistics(const std::shared_ptr<Table> table);

  /**
   * Table statistics should not be copied by other actors.
   * Copy constructor not private as copy is used by make_shared.
   */
  TableStatistics(const TableStatistics& table_statistics) = default;

  /**
   * Create the TableStatistics by explicitly specifying its underlying data. Intended for statistics tests or to
   * supply mocked statistics to a MockNode
   */
  TableStatistics(const TableType table_type, float row_count,
                  const std::vector<std::shared_ptr<BaseColumnStatistics>>& column_statistics);

  TableType table_type() const;

  /**
   * Returns the expected row_count of the output of the corresponding operator.
   * See _distinct_count declaration below or explanation of float type.
   */
  float row_count() const;

  // Returns the number of valid rows (using approximate count of deleted rows)
  uint64_t approx_valid_row_count() const;

  const std::vector<std::shared_ptr<BaseColumnStatistics>>& column_statistics() const;

  /**
   * Generate table statistics for the operator table scan table scan.
   */
  virtual std::shared_ptr<TableStatistics> predicate_statistics(
      const ColumnID column_id, const PredicateCondition predicate_condition, const AllParameterVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt);

  /**
   * Generate table statistics for a cross join.
   */
  virtual std::shared_ptr<TableStatistics> generate_cross_join_statistics(
      const std::shared_ptr<TableStatistics>& right_table_stats);

  /**
   * Generate table statistics for joins with two column predicates.
   */
  virtual std::shared_ptr<TableStatistics> generate_predicated_join_statistics(
      const std::shared_ptr<TableStatistics>& right_table_stats, const JoinMode mode, const ColumnIDPair column_ids,
      const PredicateCondition predicate_condition);

  // Increases the (approximate) count of invalid rows in the table (caused by deletes).
  void increment_invalid_row_count(uint64_t count);

 protected:
  std::shared_ptr<BaseColumnStatistics> _get_or_generate_column_statistics(const ColumnID column_id) const;

  void _create_all_column_statistics() const;

  /**
   * Resets the pointer variable _table after checking that the table is no longer needed. If the pointer is null, all
   * column statistics have been created. This check is useful during generation of join statistics as then all column
   * statistics have to be created.
   */
  void _reset_table_ptr();

  /**
   * Calculates how many null values are added to the columns of table 2 when outer joining with table 1.
   * @param row_count: row count of table 1
   * @param col_stats: column statistics of column x from table 1 used in the join predicate
   * @param predicate_column_distinct_count: distinct count of column x after join was performed
   * @return Number of additional null values for columns of table 2.
   */
  float _calculate_added_null_values_for_outer_join(const float row_count,
                                                    const std::shared_ptr<BaseColumnStatistics> col_stats,
                                                    const float predicate_column_distinct_count) const;

  /**
   * Adjusts the null-value ratio of column statistics during outer joins. The column statistics are passed via
   * iterators from a vector specifying which column statistics should be adjusted.
   * @param col_begin: iterator pointing to the first column statistics within a vector to be adjusted
   * @param col_end: iterator pointing to the next column statistics after the last column statistics to be adjusted
   * @param row_count: row count of the columns before the join
   * @param null_value_no: number of null-values to add to each column
   * @param new_row_count: row count of the new join table
   */
  void _adjust_null_value_ratio_for_outer_join(
      const std::vector<std::shared_ptr<BaseColumnStatistics>>::iterator col_begin,
      const std::vector<std::shared_ptr<BaseColumnStatistics>>::iterator col_end, const float row_count,
      const float null_value_no, const float new_row_count);

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics
  // not const as table pointer can be reset once all column statistics have been created
  // (e.g. for composite tables resulting from joins)
  std::weak_ptr<Table> _table;

  TableType _table_type;

  // row count is not an integer as it is a predicted value
  // it is multiplied with selectivity factor of a corresponding operator to predict the operator's output
  // precision is lost, if row count is rounded
  float _row_count = 0.0f;

  // Stores the number of invalid (deleted) rows.
  // This is currently not an atomic due to performance considerations.
  // It is simply used as an estimate for the optimizer, and therefore does not need to be exact.
  uint64_t _approx_invalid_row_count{0};

  mutable std::vector<std::shared_ptr<BaseColumnStatistics>> _column_statistics;

  friend std::ostream& operator<<(std::ostream& os, TableStatistics& obj);
};

std::ostream& operator<<(std::ostream& os, TableStatistics& obj);

// default selectivity factors for assumption of uniform distribution
constexpr float DEFAULT_SELECTIVITY = 0.5f;
constexpr float DEFAULT_LIKE_SELECTIVITY = 0.1f;
// values below are taken from paper "Access path selection in a relational database management system",
// P. Griffiths Selinger, 1979
constexpr float DEFAULT_OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
constexpr float DEFAULT_BETWEEN_SELECTIVITY = 0.25f;

}  // namespace opossum
