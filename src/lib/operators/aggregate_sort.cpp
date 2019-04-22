#include "aggregate_sort.hpp"

#include <memory>
#include <vector>

#include "aggregate/aggregate_traits.hpp"
#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "operators/sort.hpp"
#include "storage/segment_iterate.hpp"
#include "table_wrapper.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& in,
                             const std::vector<AggregateColumnDefinition>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

const std::string AggregateSort::name() const { return "AggregateSort"; }

/**
 * Calculates the value for the <code>aggregate_index</code>'th aggregate.
 * To do this, we iterate over all segments of the corresponding column.
 * The aggregate values (one per group-by-combination) are built incrementally.
 * Every time we reach the beginning of a new group-by-combination,
 * we store the aggregate value for the current (now previous) group.
 *
 * @tparam ColumnType the type of the input column to aggregate on
 * @tparam AggregateType the type of the aggregate (=output column)
 * @tparam function the aggregate function, as type parameter - e.g. AggregateFunction::MIN, AVG, COUNT, ...
 * @param group_boundaries the row ids where a new combination in the sorted table begins
 * @param aggregate_index determines which aggregate to calculate (from _aggregates)
 * @param aggregate_function the aggregate function - as callable object. This should always be the same as <code>function</code>
 * @param sorted_table the input table, sorted by the group by columns
 */
template <typename ColumnType, typename AggregateType, AggregateFunction function>
void AggregateSort::_aggregate_values(const std::set<RowID>& group_boundaries, const uint64_t aggregate_index,
                                      const std::shared_ptr<const Table>& sorted_table) {
  auto aggregate_function = AggregateFunctionBuilder<ColumnType, AggregateType, function>().get_aggregate_function();

  // We already know beforehand how many aggregate values (=group-by-combinations) we have to calculate
  const size_t num_groups = group_boundaries.size() + 1;

  // Vectors to store aggregate values (and if they are NULL) for later usage in value segments
  auto aggregate_results = std::vector<AggregateType>(num_groups);
  auto aggregate_null_values = std::vector<bool>(num_groups);

  // Variables needed for the aggregates. Not all variables are needed for all aggregates

  // Row counts per group, ex- and including null values. Needed for count (<column>/*) and average
  uint64_t value_count = 0u;
  uint64_t value_count_with_null = 0u;

  // All unique values found. Needed for count distinct
  std::unordered_set<ColumnType> unique_values;

  // The number of the current group-by-combination. Used as offset when storing values
  uint64_t aggregate_group_index = 0u;

  const auto& chunks = sorted_table->chunks();

  std::optional<AggregateType> current_aggregate_value;
  ChunkID current_chunk_id{0};
  if (function == AggregateFunction::Count && !_aggregates[aggregate_index].column) {
    /*
     * Special COUNT(*) implementation.
     * We do not need to care about null values for COUNT(*).
     * Because of this, we can simply calculate the number of elements per group (=COUNT(*))
     * by calculating the distance between the first and the last corresponding row (+1) in the table.
     * This results in a runtime of O(output rows) rather than O(input rows), which can be quite significant.
     */
    auto current_group_begin_pointer = RowID{ChunkID{0u}, ChunkOffset{0}};
    for (const auto& group_boundary : group_boundaries) {
      if (current_group_begin_pointer.chunk_id == group_boundary.chunk_id) {
        // Group is located within a single chunk
        value_count_with_null = group_boundary.chunk_offset - current_group_begin_pointer.chunk_offset;
      } else {
        // Group is spread over multiple chunks
        uint64_t count = 0;
        count += chunks[current_group_begin_pointer.chunk_id]->size() - current_group_begin_pointer.chunk_offset;
        for (auto chunk_id = current_group_begin_pointer.chunk_id + 1; chunk_id < group_boundary.chunk_id; chunk_id++) {
          count += chunks[chunk_id]->size();
        }
        count += group_boundary.chunk_offset + 1;
        value_count_with_null = count;
      }
      _set_and_write_aggregate_value<AggregateType, function>(
          aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
          value_count, value_count_with_null, unique_values.size());
      current_group_begin_pointer = group_boundary;
      aggregate_group_index++;
    }
    // Compute value count with null values for the last iteration
    value_count_with_null =
        chunks[current_group_begin_pointer.chunk_id]->size() - current_group_begin_pointer.chunk_offset;
    for (size_t chunk_id = current_group_begin_pointer.chunk_id + 1; chunk_id < chunks.size(); chunk_id++) {
      value_count_with_null += chunks[chunk_id]->size();
    }

  } else {
    /*
     * High-level overview of the algorithm:
     *
     * We already know at which RowIDs a new group (=group-by-combination) begins,
     * it is stored in group_boundaries.
     * The base idea is:
     *
     * Iterate over every value in the aggregate column, and keep track of the current RowID
     *   if (current row id == start of next group)
     *     write aggregate value of the just finished group
     *     reset helper variables
     *
     *   update helper variables
     *
     */
    auto group_boundary_iter = group_boundaries.cbegin();
    const auto column_id = _aggregates[aggregate_index];
    const auto column = column_id.column;
    for (const auto& chunk : chunks) {
      auto segment = chunk->get_segment(*column);
      segment_iterate<ColumnType>(*segment, [&](const auto& position) {
        const auto row_id = RowID{current_chunk_id, position.chunk_offset()};
        const auto is_new_group = !group_boundaries.empty() && row_id == *group_boundary_iter;
        const auto new_value = position.value();
        if (is_new_group) {
          // New group is starting. Store the aggregate value of the just finished group
          _set_and_write_aggregate_value<AggregateType, function>(
              aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
              value_count, value_count_with_null, unique_values.size());

          // Reset helper variables
          current_aggregate_value = std::optional<AggregateType>();
          unique_values.clear();
          value_count = 0u;
          value_count_with_null = 0u;

          // Update indexing variables
          aggregate_group_index++;
          group_boundary_iter++;
        }

        // Update helper variables
        if (!position.is_null()) {
          aggregate_function(new_value, current_aggregate_value);
          value_count++;
          if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
            unique_values.insert(new_value);
          }
        }
        value_count_with_null++;
      });
      current_chunk_id++;
    }
  }
  // Aggregate value for the last group was not written yet
  _set_and_write_aggregate_value<AggregateType, function>(
      aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
      value_count, value_count_with_null, unique_values.size());

  // Store the aggregate values in a value segment
  _output_segments[aggregate_index + _groupby_column_ids.size()] =
      std::make_shared<ValueSegment<AggregateType>>(aggregate_results, aggregate_null_values);
}

/**
 * This is a generic method to add an aggregate value to the current aggregate value vector (<code>aggregate_results</code>).
 * While adding the new value itself is easy, deciding on what exactly the new value is might be not that easy.
 *
 * Sometimes it is simple, e.g. for Min, Max and Sum the value we want to add is directly the <code>current_aggregate_value</code>.
 * But sometimes it is more complicated, e.g. Avg effectively only calculates a sum, and we manually need to divide the sum by the number of (non-null) elements that contributed to it.
 * Further, Count and CountDistinct do not use <code>current_aggregate_value</code>, instead we pass <code>value_count</code> and <code>value_count_with_null</code> directly to the function.
 * The latter might look ugly at the first sight, but we need those counts (or at least <code>value_count</code>) anyway for Avg, and it is consistent with the hash aggregate.
 *
 * The function is "generic" in the sense that it works for every aggregate type.
 * This way, we can have a single call that is valid for all aggregate functions, which increases readability.
 * We pay for this easy call with more function parameters than we might need.
 * As a result, we have to mark most of the parameters with [[maybe_unused]] to avoid compiler complaints.
 * Note: if a parameter is not used for a certain aggregate type, it might not contain meaningful values.
 *
 *
 * @tparam AggregateType the data type of the aggregate result
 * @tparam function the aggregate function - e.g. AggregateFunction::Min, Max, ...
 * @param aggregate_results the vector where the aggregate values should be stored
 * @param aggregate_null_values the vector indicating whether a specific aggregate value is null
 * @param aggregate_group_index the offset to use for <code>aggregate_results</code> and <code>aggregate_null_values</code>
 * @param aggregate_index current aggregate's offset in <code>_aggregates</code>
 * @param current_aggregate_value the value of the aggregate (return value of the aggregate function) - used by all except COUNT (all versions)
 * @param value_count the number of non-null values - used by COUNT(<name>) and AVG
 * @param value_count_with_null the number of rows  - used by COUNT(*)
 * @param unique_value_count the number of unique values
 */
template <typename AggregateType, AggregateFunction function>
void AggregateSort::_set_and_write_aggregate_value(
    std::vector<AggregateType>& aggregate_results, std::vector<bool>& aggregate_null_values,
    const uint64_t aggregate_group_index, [[maybe_unused]] const uint64_t aggregate_index,
    std::optional<AggregateType>& current_aggregate_value, [[maybe_unused]] const uint64_t value_count,
    [[maybe_unused]] const uint64_t value_count_with_null, [[maybe_unused]] const uint64_t unique_value_count) const {
  if constexpr (function == AggregateFunction::Count) {  // NOLINT
    if (this->_aggregates[aggregate_index].column) {
      // COUNT(<name>), so exclude null values
      current_aggregate_value = value_count;
    } else {
      // COUNT(*), so include null values
      current_aggregate_value = value_count_with_null;
    }
  }
  if constexpr (function == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>) {  // NOLINT
    // this ignores the case of Avg on strings, but we check in _on_execute() this does not happen

    if (value_count == 0) {
      // there are no non-null values, the average itself must be null (otherwise division by 0)
      current_aggregate_value = std::optional<AggregateType>();
    } else {
      // normal average calculation
      current_aggregate_value = *current_aggregate_value / value_count;
    }
  }
  if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
    current_aggregate_value = unique_value_count;
  }

  // store whether the value is a null value
  aggregate_null_values[aggregate_group_index] = !current_aggregate_value;
  if (current_aggregate_value) {
    // only store non-null values
    aggregate_results[aggregate_group_index] = *current_aggregate_value;
  }
}

/**
 * Executes the aggregation.
 * High-level overview:
 *
 * Make sure the aggregates are valid
 * Build the output table definition
 * if empty
 *   return (empty) result table
 *
 * Sort the input table after all group by columns.
 *  Currently, this is done using multiple passes of the Sort operator (which is stable)
 *  For future implementations, the fact that the input table is already sorted could be used to skip this step.
 *    See https://github.com/hyrise/hyrise/issues/1519 for a discussion about operators using sortedness.
 *
 * Find the group boundaries
 *  Our table is now sorted after all group by columns.
 *  As a result, all rows that fall into the same group are consecutive within that table.
 *  Thus, we can easily find all group boundaries (specifically their first element)
 *  by iterating over the group by columns and storing RowIDs of rows where the value of any group by column changes.
 *  The result is a (ordered) set, its entries marking the beginning of a new group-by-combination.
 *
 * Write the values of group by columns for each group into a ValueSegment
 *  For each group by column
 *   Iterate over the group boundaries (RowIDs) and output the value
 *    Note: This cannot be merged with the first iteration (finding group boundaries).
 *          This is because we have to output values for all RowIDs where ANY group by column changes,
 *          not only the one we currently iterate over.
 *
 * Call _aggregate_values for each aggregate, which performs the aggregation and writes the output into ValueSegments
 *
 * return result table
 *
 * @return the result table
 */
std::shared_ptr<const Table> AggregateSort::_on_execute() {
  const auto input_table = input_table_left();

  // Check for invalid aggregates
  _validate_aggregates();

  // Create group by column definitions
  for (const auto& column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id), true);
  }

  // Create aggregate column definitions
  ColumnID column_index{0};
  for (const auto& aggregate : _aggregates) {
    const auto column = aggregate.column;

    /*
     * Special case for COUNT(*), which is the only case where !column is true:
     * Usually, the data type of the aggregate can depend on the data type of the corresponding input column.
     * For example, the sum of ints is an int, while the sum of doubles is an double.
     * For COUNT(*), the aggregate type is always an integral type, regardless of the input type.
     * As the input type does not matter and we do not even have an input column,
     * but the function call expects an input type, we choose Int arbitrarily.
     * This is NOT the result type of COUNT(*), which is Long.
     */
    const auto data_type = !column ? DataType::Int : input_table->column_data_type(*column);

    resolve_data_type(data_type, [&, column_index](auto type) {
      _create_aggregate_column_definitions(type, column_index, aggregate.function);
    });

    ++column_index;
  }

  auto result_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);

  /*
   * Handle empty input table according to the SQL standard
   *  if group by columns exist -> empty result
   *  else -> one row with default values
   *
   * TODO(anyone) this code could probably be shared by the aggregate implementations.
   * As of now, the hash aggregate handles this edge case in write_aggregate_output().
   * However, HashAggregate::write_aggregate_output writes both agggregate column definitions and actual values.
   * Separating those two steps would allow us to reuse AggregateSort::create_aggregate_column_definitions(),
   * as well as the code below.
   */
  if (input_table->empty()) {
    if (_groupby_column_ids.empty()) {
      std::vector<AllTypeVariant> default_values;
      for (const auto& aggregate : _aggregates) {
        if (aggregate.function == AggregateFunction::Count || aggregate.function == AggregateFunction::CountDistinct) {
          default_values.emplace_back(AllTypeVariant{0});
        } else {
          default_values.emplace_back(NULL_VALUE);
        }
      }
      result_table->append(default_values);
    }

    return result_table;
  }

  /*
   * NOTE: There are some cases in the TPCH-Queries, where we get tables with more columns than required.
   * Required columns are all columns that we group by, as well as all columns we aggregate on.
   * All other columns are superfluous, as they will not be part of the result table (and thus not accessible for later operators).
   *
   * This could lead to performance issues as the Sort operator needs to materialize more data.
   * We believe this to be a minor issue:
   * It seems that in most cases the optimizer puts a projection before the aggregate operator,
   * and in cases where it does not, the table size is small.
   * However, we did not benchmark it, so we cannot prove it.
   */

  // Sort input table consecutively by the group by columns (stable sort)
  auto sorted_table = input_table;
  for (const auto& column_id : _groupby_column_ids) {
    const auto sorted_wrapper = std::make_shared<TableWrapper>(sorted_table);
    sorted_wrapper->execute();
    Sort sort = Sort(sorted_wrapper, column_id);
    sort.execute();
    sorted_table = sort.get_output();
  }

  _output_segments.resize(_aggregates.size() + _groupby_column_ids.size());

  /*
   * Find all RowIDs where a value in any group by column changes compared to the previous row,
   * as those are exactly the boundaries of the different groups.
   * Everytime a value changes in any of the group by columns, we add the current position to the set.
   * We are aware that std::set is known to be not the most efficient,
   * but our profiling revealed that the current implementation is not a bottleneck.
   * Currently, the vast majority of execution time is spent with sorting.
   * Moreover, this set grows only to O(output) many elements, so inserts and lookups should stay reasonably fast.
   *
   *
   * If this does become a performance problem at some point, one could try e.g. a vector of vectors,
   * with one vector per column.
   * Each vector stores all RowIDs where the corresponding column changes.
   * Afterwards, the vectors have to be merged and deduplicated (presorted list merge).
   * We have not benchmarked this solution, so we do not know how it performs in comparison to the current one.
   *
   * General note: group_boundaries contains the first entry of a new group,
   *               or in other words: the last entry of a group + 1 row, similar to vector::end()
   *
   *               It is (in _aggregate_values) used as indicator that a group has ended just before this row.
   *               As a result, the start of the very first group is not represented in group_boundaries,
   *               as there was no group before that ended.
   *               Further, the end of the last group is not represented as well.
   *               This is because no new group starts after it.
   *               So in total, group_boundaries will contain one element less than there are groups.
   */
  std::set<RowID> group_boundaries;
  const auto& chunks = sorted_table->chunks();
  for (const auto& column_id : _groupby_column_ids) {
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      std::optional<ColumnDataType> previous_value;
      ChunkID chunk_id{0};

      /*
       * Initialize previous_value to the first value in the table, so we avoid considering it a value change.
       * We do not want to consider it as a value change, because we the first row should not be part of the boundaries.
       * For the reasoning behind it see above.
       * We are aware that operator[] is slow, however, for one value it should be faster than segment_iterate_filtered.
       */
      const auto& first_segment = chunks[0]->get_segment(column_id);
      const auto& first_value = (*first_segment)[0];
      if (variant_is_null(first_value)) {
        previous_value.reset();
      } else {
        previous_value.emplace(type_cast_variant<ColumnDataType>(first_value));
      }

      // Iterate over all chunks and insert RowIDs when values change
      for (const auto& chunk : chunks) {
        const auto& segment = chunk->get_segment(column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (previous_value.has_value() == position.is_null() ||
              (previous_value.has_value() && !position.is_null() && position.value() != *previous_value)) {
            group_boundaries.insert(RowID{chunk_id, position.chunk_offset()});
            if (position.is_null()) {
              previous_value.reset();
            } else {
              previous_value.emplace(position.value());
            }
          }
        });
        chunk_id++;
      }
    });
  }

  /*
   * For all group by columns
   *   For each group by column
   *     output the column value at the start of the group (it is per definition the same in the whole group)
   * Write outputted values into the result table
   */
  size_t groupby_index = 0;
  for (const auto& column_id : _groupby_column_ids) {
    auto group_boundary_iter = group_boundaries.cbegin();
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto values = std::vector<ColumnDataType>(group_boundaries.size() + 1);
      auto null_values = std::vector<bool>(group_boundaries.size() + 1);

      for (size_t value_index = 0; value_index < values.size(); value_index++) {
        RowID group_start;
        if (value_index == 0) {
          // First group starts in the first row, but there is no corresponding entry in the set. See above for reasons.
          group_start = RowID{ChunkID{0}, 0};
        } else {
          group_start = *group_boundary_iter;
          group_boundary_iter++;
        }

        const auto& chunk = chunks[group_start.chunk_id];
        const auto& segment = chunk->get_segment(column_id);

        /*
         * We are aware that operator[] and AllTypeVariant are known to be inefficient.
         * However, for accessing a single value it is probably more efficient than segment_iterate_filtered,
         * besides being more readable.
         * We cannot use segment_iterate_filtered with the whole group_boundaries (converted to a PosList).
         * This is because the RowIDs in group_boundaries can reference multiple chunks.
         */
        const auto& value = (*segment)[group_start.chunk_offset];

        null_values[value_index] = variant_is_null(value);
        if (!null_values[value_index]) {
          // Only store non-null values
          values[value_index] = type_cast_variant<ColumnDataType>(value);
        }
      }

      // Write group by segments
      _output_segments[groupby_index] = std::make_shared<ValueSegment<ColumnDataType>>(values, null_values);
    });
    groupby_index++;
  }

  // Call _aggregate_values for each aggregate
  uint64_t aggregate_index = 0;
  for (const auto& aggregate : _aggregates) {
    /*
     * Special case for COUNT(*), which is the only case where !aggregate.column is true:
     * Usually, the data type of the aggregate can depend on the data type of the corresponding input column.
     * For example, the sum of ints is an int, while the sum of doubles is an double.
     * For COUNT(*), the aggregate type is always an integral type, regardless of the input type.
     * As the input type does not matter and we do not even have an input column,
     * but the function call expects an input type, we choose Int arbitrarily.
     * This is NOT the result type of COUNT(*), which is Long.
     */
    const auto data_type = !aggregate.column ? DataType::Int : input_table->column_data_type(*aggregate.column);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      /*
       * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
       * The problem we encountered: We cannot simply hand aggregate.function into the call of _aggregate_values.
       * The reason: the compiler wants to know at compile time which of the templated versions need to be called.
       * However, aggregate.function is something that is only available at runtime,
       * so the compiler cannot know its value and thus not deduce the correct method call.
       */
      switch (aggregate.function) {
        case AggregateFunction::Min: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Min>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Min>(group_boundaries, aggregate_index,
                                                                                   sorted_table);
          break;
        }
        case AggregateFunction::Max: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Max>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Max>(group_boundaries, aggregate_index,
                                                                                   sorted_table);
          break;
        }
        case AggregateFunction::Sum: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Sum>(group_boundaries, aggregate_index,
                                                                                   sorted_table);
          break;
        }

        case AggregateFunction::Avg: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Avg>(group_boundaries, aggregate_index,
                                                                                   sorted_table);
          break;
        }
        case AggregateFunction::Count: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Count>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Count>(group_boundaries, aggregate_index,
                                                                                     sorted_table);
          break;
        }
        case AggregateFunction::CountDistinct: {
          using AggregateType = typename AggregateTraits<
              ColumnDataType, AggregateFunction::CountDistinct>::AggregateType;  // NOLINT(whitespace/line_length)
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>(
              group_boundaries, aggregate_index, sorted_table);
          break;
        }
      }
    });

    aggregate_index++;
  }

  // Append output to result table
  result_table->append_chunk(_output_segments);

  return result_table;
}

std::shared_ptr<AbstractOperator> AggregateSort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AggregateSort>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateSort::_on_cleanup() {}

template <typename ColumnType>
void AggregateSort::_create_aggregate_column_definitions(boost::hana::basic_type<ColumnType> type,
                                                         ColumnID column_index, AggregateFunction function) {
  /*
   * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
   * There is a similar switch statement in _on_execute for calling _aggregate_values.
   * See the comment there for reasoning.
   */
  switch (function) {
    case AggregateFunction::Min:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Min>(column_index);
      break;
    case AggregateFunction::Max:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Max>(column_index);
      break;
    case AggregateFunction::Sum:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Sum>(column_index);
      break;
    case AggregateFunction::Avg:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Avg>(column_index);
      break;
    case AggregateFunction::Count:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Count>(column_index);
      break;
    case AggregateFunction::CountDistinct:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::CountDistinct>(column_index);
      break;
  }
}

/*
 * TODO(anyone) this function could be shared by the aggregate implementations.
 *
 * Most of this function is copied from HashAggregate::write_aggregate_output().
 * However, the function in the hash aggregate does more than just creating aggregate colum definitions.
 * As the name says, it also writes the aggregate output (values),
 *  which the sort aggregate already does in another place.
 * To reduce code duplication, the hash aggregate could be refactored to separate creating column definitions
 *  and writing the actual output.
 *  This would also allow to reuse the code for handling an empty input table (see _on_execute()).
 */
template <typename ColumnType, AggregateFunction function>
void AggregateSort::create_aggregate_column_definitions(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  auto aggregate_data_type = AggregateTraits<ColumnType, function>::AGGREGATE_DATA_TYPE;

  const auto& aggregate = _aggregates[column_index];

  if (aggregate_data_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    aggregate_data_type = input_table_left()->column_data_type(*aggregate.column);
  }

  // Generate column name, TODO(anybody), actually, the AggregateExpression can do this, but the Aggregate operator
  // doesn't use Expressions, yet
  std::stringstream column_name_stream;
  if (aggregate.function == AggregateFunction::CountDistinct) {
    column_name_stream << "COUNT(DISTINCT ";
  } else {
    column_name_stream << aggregate.function << "(";
  }

  if (aggregate.column) {
    column_name_stream << input_table_left()->column_name(*aggregate.column);
  } else {
    column_name_stream << "*";
  }
  column_name_stream << ")";

  constexpr bool NEEDS_NULL = (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct);
  _output_column_definitions.emplace_back(column_name_stream.str(), aggregate_data_type, NEEDS_NULL);
}
}  // namespace opossum
