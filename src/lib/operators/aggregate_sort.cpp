#include "aggregate_sort.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregate/window_function_traits.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/sort.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "table_wrapper.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT

std::shared_ptr<const Table> sort_table_by_column_ids(const std::shared_ptr<const Table>& table_to_sort,
                                                      const std::vector<ColumnID>& column_ids) {
  // Create sort definition vector from group by list for sort operator
  auto sort_definitions = std::vector<SortColumnDefinition>{};
  sort_definitions.reserve(column_ids.size());
  for (const auto& column_id : column_ids) {
    sort_definitions.emplace_back(column_id, SortMode::Ascending);
  }

  const auto table_wrapper = std::make_shared<TableWrapper>(table_to_sort);
  table_wrapper->execute();

  const auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions);
  sort->execute();

  return sort->get_output();
}

}  // namespace

namespace hyrise {

AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateSort::name() const {
  static const auto name = std::string{"AggregateSort"};
  return name;
}

/**
 * Calculates the value for the aggregate_index'th aggregate.
 * To do this, we iterate over all segments of the corresponding column.
 * The aggregate values (one per group-by-combination) are built incrementally.
 * Every time we reach the beginning of a new group-by-combination,
 * we store the aggregate value for the current (now previous) group.
 *
 * @tparam ColumnType the type of the input column to aggregate on
 * @tparam AggregateType the type of the aggregate (=output column)
 * @tparam aggregate_function as type parameter - e.g. WindowFunction::MIN, AVG, COUNT, ...
 * @param group_boundaries the row ids where a new combination in the sorted table begins
 * @param aggregate_index determines which aggregate to calculate (from _aggregates)
 * @param aggregate_function the aggregate function - as callable object. This should always be the same as aggregate_function
 * @param sorted_table the input table, sorted by the group by columns
 */
template <typename ColumnType, typename AggregateType, WindowFunction aggregate_function>
void AggregateSort::_aggregate_values(const std::set<RowID>& group_boundaries, const uint64_t aggregate_index,
                                      const std::shared_ptr<const Table>& sorted_table) {
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*_aggregates[aggregate_index]->argument());
  const auto input_column_id = pqp_column.column_id;

  auto aggregator = WindowFunctionBuilder<ColumnType, AggregateType, aggregate_function>().get_aggregate_function();

  // We already know beforehand how many aggregate values (=group-by-combinations) we have to calculate
  const auto num_groups = group_boundaries.size() + 1;

  // Vectors to store aggregate values (and if they are NULL) for later usage in value segments
  auto aggregate_results = pmr_vector<AggregateType>(num_groups);
  auto aggregate_null_values = pmr_vector<bool>(num_groups);

  // Variables needed for the aggregates. Not all variables are needed for all aggregates

  // Row counts per group, ex- and including null values. Needed for count (<column>/*) and average
  auto value_count = uint64_t{0};
  auto value_count_with_null = uint64_t{0};

  // All unique values found. Needed for count distinct
  auto unique_values = std::unordered_set<ColumnType>{};

  // The number of the current group-by-combination. Used as offset when storing values
  auto aggregate_group_index = uint64_t{0};

  const auto chunk_count = sorted_table->chunk_count();

  auto accumulator = AggregateAccumulator<aggregate_function, AggregateType>{};
  auto current_chunk_id = ChunkID{0};
  if (aggregate_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID) {
    /*
     * Special COUNT(*) implementation.
     * We do not need to care about null values for COUNT(*).
     * Because of this, we can simply calculate the number of elements per group (=COUNT(*))
     * by calculating the distance between the first and the last corresponding row (+1) in the table.
     * This results in a runtime of O(output rows) rather than O(input rows), which can be quite significant.
     */
    auto current_group_begin_pointer = RowID{ChunkID{0}, ChunkOffset{0}};
    for (const auto& group_boundary : group_boundaries) {
      if (current_group_begin_pointer.chunk_id == group_boundary.chunk_id) {
        // Group is located within a single chunk
        value_count_with_null = group_boundary.chunk_offset - current_group_begin_pointer.chunk_offset;
      } else {
        // Group is spread over multiple chunks
        auto count = uint64_t{0};
        count += sorted_table->get_chunk(current_group_begin_pointer.chunk_id)->size() -
                 current_group_begin_pointer.chunk_offset;
        for (auto chunk_id = ChunkID{current_group_begin_pointer.chunk_id + 1}; chunk_id < group_boundary.chunk_id;
             chunk_id++) {
          count += sorted_table->get_chunk(chunk_id)->size();
        }
        count += group_boundary.chunk_offset + 1;
        value_count_with_null = count;
      }
      _set_and_write_aggregate_value<AggregateType, aggregate_function>(
          aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, accumulator, value_count,
          value_count_with_null, unique_values.size());
      current_group_begin_pointer = group_boundary;
      aggregate_group_index++;
    }
    // Compute value count with null values for the last iteration
    value_count_with_null = sorted_table->get_chunk(current_group_begin_pointer.chunk_id)->size() -
                            current_group_begin_pointer.chunk_offset;
    for (auto chunk_id = ChunkID{current_group_begin_pointer.chunk_id + 1}; chunk_id < chunk_count; ++chunk_id) {
      value_count_with_null += sorted_table->get_chunk(chunk_id)->size();
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
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto segment = sorted_table->get_chunk(chunk_id)->get_segment(input_column_id);
      segment_iterate<ColumnType>(*segment, [&](const auto& position) {
        const auto row_id = RowID{current_chunk_id, position.chunk_offset()};
        const auto is_new_group = group_boundary_iter != group_boundaries.cend() && row_id == *group_boundary_iter;
        const auto& new_value = position.value();
        if (is_new_group) {
          // New group is starting. Store the aggregate value of the just finished group
          _set_and_write_aggregate_value<AggregateType, aggregate_function>(
              aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, accumulator,
              value_count, value_count_with_null, unique_values.size());

          // Reset helper variables
          accumulator = {};
          unique_values.clear();
          value_count = 0;
          value_count_with_null = 0;

          // Update indexing variables
          ++aggregate_group_index;
          ++group_boundary_iter;
        }

        // Update helper variables
        if (!position.is_null()) {
          aggregator(new_value, value_count, accumulator);
          ++value_count;
          if constexpr (aggregate_function == WindowFunction::CountDistinct) {
            unique_values.insert(new_value);
          } else if constexpr (aggregate_function == WindowFunction::Any) {
            // Gathering the group's first value for ANY() is sufficient
            return;
          }
        }
        ++value_count_with_null;
      });
      ++current_chunk_id;
    }
  }
  // Aggregate value for the last group was not written yet
  _set_and_write_aggregate_value<AggregateType, aggregate_function>(
      aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, accumulator, value_count,
      value_count_with_null, unique_values.size());

  // Store the aggregate values in a value segment
  if (_output_column_definitions.at(aggregate_index + _groupby_column_ids.size()).nullable) {
    _output_segments[aggregate_index + _groupby_column_ids.size()] =
        std::make_shared<ValueSegment<AggregateType>>(std::move(aggregate_results), std::move(aggregate_null_values));
  } else {
    _output_segments[aggregate_index + _groupby_column_ids.size()] =
        std::make_shared<ValueSegment<AggregateType>>(std::move(aggregate_results));
  }
}

/**
 * This is a generic method to add an aggregate value to the current aggregate value vector (`aggregate_results`).
 * While adding the new value itself is easy, deciding on what exactly the new value is might be not that easy.
 *
 * Sometimes it is simple, e.g. for Min, Max and Sum the value we want to add is directly the `accumulator`.
 * But sometimes it is more complicated, e.g. Avg effectively only calculates a sum, and we manually need to divide the
 * sum by the number of (non-null) elements that contributed to it. Further, Count and CountDistinct do not use
 * `current_x_aggregate`, instead we pass `value_count` and `value_count_with_null` directly to the function. The latter
 * might look ugly at the first sight, but we need those counts (or at least `value_count`) anyway for Avg, and it is
 * consistent with the hash aggregate.
 *
 * The function is "generic" in the sense that it works for every aggregate type.
 * This way, we can have a single call that is valid for all aggregate functions, which increases readability.
 * We pay for this easy call with more function parameters than we might need.
 * As a result, we have to mark most of the parameters with [[maybe_unused]] to avoid compiler complaints.
 * Note: if a parameter is not used for a certain aggregate type, it might not contain meaningful values.
 *
 *
 * @tparam AggregateType the data type of the aggregate result
 * @tparam aggregate_function as type parameter - e.g. WindowFunction::Min, Max, ...
 * @param aggregate_results the vector where the aggregate values should be stored
 * @param aggregate_null_values the vector indicating whether a specific aggregate value is null
 * @param aggregate_group_index the offset to use for `aggregate_results` and `aggregate_null_values`
 * @param aggregate_index current aggregate's offset in `_aggregates`
 * @param accumulator the value of the aggregate (return value of the aggregate function) - used by all except
 *                    COUNT(<expr>) and COUNT(*)
 * @param value_count the number of non-null values - used by COUNT(<expr>), AVG
 * @param value_count_with_null the number of rows  - used by COUNT(*)
 * @param unique_value_count the number of unique values
 */
template <typename AggregateType, WindowFunction aggregate_function>
void AggregateSort::_set_and_write_aggregate_value(
    pmr_vector<AggregateType>& aggregate_results, pmr_vector<bool>& aggregate_null_values,
    const uint64_t aggregate_group_index, [[maybe_unused]] const uint64_t aggregate_index,
    AggregateAccumulator<aggregate_function, AggregateType>& accumulator, [[maybe_unused]] const uint64_t value_count,
    [[maybe_unused]] const uint64_t value_count_with_null, [[maybe_unused]] const uint64_t unique_value_count) const {
  auto is_null = value_count == 0;

  if constexpr (aggregate_function == WindowFunction::Count) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*this->_aggregates[aggregate_index]->argument());
    const auto input_column_id = pqp_column.column_id;

    if (input_column_id != INVALID_COLUMN_ID) {
      // COUNT(<expr>), so exclude null values
      accumulator = value_count;
    } else {
      // COUNT(*), so include null values
      accumulator = value_count_with_null;
    }

    // COUNT is never NULL
    is_null = false;
  }

  if constexpr (aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    if (value_count > 0) {
      accumulator = accumulator / static_cast<AggregateType>(value_count);
    }
  }

  if constexpr (aggregate_function == WindowFunction::CountDistinct) {
    accumulator = unique_value_count;

    // COUNT is never NULL
    is_null = false;
  }

  if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
    if (!std::is_same_v<AggregateType, pmr_string>) {
      if (value_count >= 2) {
        aggregate_results[aggregate_group_index] = accumulator[3];
      } else {
        // STDDEV_SAMP is undefined for lists with less than two elements
        is_null = true;
      }
    } else {
      Fail("StandardDeviationSample does not work for strings");
    }
  } else {
    aggregate_results[aggregate_group_index] = accumulator;
  }

  aggregate_null_values[aggregate_group_index] = is_null;
}

Segments AggregateSort::_get_segments_of_chunk(const std::shared_ptr<const Table>& input_table,
                                               const ChunkID chunk_id) {
  auto segments = Segments{};
  const auto column_count = input_table->column_count();
  segments.reserve(column_count);
  const auto chunk = input_table->get_chunk(chunk_id);

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    segments.emplace_back(chunk->get_segment(column_id));
  }
  return segments;
}

std::shared_ptr<Table> AggregateSort::_sort_table_chunk_wise(const std::shared_ptr<const Table>& input_table,
                                                             const std::vector<ColumnID>& groupby_column_ids) {
  auto output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References);

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    const auto& chunk_sorted_by = input_table->get_chunk(chunk_id)->individually_sorted_by();

    // We can skip sorting the chunk only if we group by a single column and the chunk is sorted by that column. We do
    // not store information about cascadingly sorted chunks, which we would need for skipping the sort step with
    // multiple group by columns. The sort mode can be neglected as the aggregate only requires consecutiveness of
    // values.
    const auto single_column_group_by = groupby_column_ids.size() == 1;
    const auto chunk_sorted_by_first_group_by_column =
        std::find_if(chunk_sorted_by.cbegin(), chunk_sorted_by.cend(), [&](const auto& sort_definition) {
          return groupby_column_ids[0] == sort_definition.column;
        }) != chunk_sorted_by.cend();

    if (single_column_group_by && chunk_sorted_by_first_group_by_column) {
      if (input_table->type() == TableType::Data) {
        // Since the chunks that actually have to be sorted will be returned as referencing chunks, we need to forward
        // the already sorted input chunks as such too, using an EntireChunkPosList.
        const auto pos_list = std::make_shared<EntireChunkPosList>(chunk_id, chunk->size());
        auto reference_segments = Segments{};
        reference_segments.reserve(column_count);

        // Create actual ReferenceSegment objects.
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
          reference_segments.push_back(ref_segment_out);
        }

        output_table->append_chunk(reference_segments);
      } else {
        // In case of a reference segment, we can directly forward it.
        output_table->append_chunk(_get_segments_of_chunk(input_table, chunk_id));
      }
    } else {
      // Creating a new table holding only the current chunk and pass it to the sort operator.
      auto single_chunk_table = std::make_shared<Table>(input_table->column_definitions(), input_table->type());
      auto segments = Segments{};
      segments.reserve(column_count);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        segments.emplace_back(chunk->get_segment(column_id));
      }
      single_chunk_table->append_chunk(segments);
      const auto sorted_single_chunk_table = sort_table_by_column_ids(single_chunk_table, groupby_column_ids);

      output_table->append_chunk(_get_segments_of_chunk(sorted_single_chunk_table, ChunkID{0}));
    }
  }

  return output_table;
}

/**
 * Executes the sort-based aggregation.
 *
 * High-level overview:
 * 1. Make sure the aggregates are valid.
 * 2. Build the output table definition.
 *    - If empty, return (empty) result table.
 * 3. Sort the input table by group by columns using the sort operator.
 *    - depending on characteristics of the input table, sorting can either be skipped (input already sorted by group
 *      by column) or sorting can be limited to chunks instead of sorting the whole table (input table is clustered)
 * 4. Find the group boundaries.
 *    - The unit of aggregation (either chunks or the whole table, depending on the table's value clustering) is now
 *      sorted by all group by columns.
 *    - As a result, all rows that fall into the same group are consecutive within that unit.
 *    - Thus, we can find all group boundaries (specifically their first element) by iterating over the group by
 *      columns and storing RowIDs of rows where the value of any group by column changes.
 *    - The result is a (sorted) vector of RowIDs, its entries marking the beginning of a new group-by-combination.
 * 5. Write the values of group by columns for each group into a ValueSegment.
 *    - For each group by column, iterate over the group boundaries (RowIDs) and output the value.
 *    - Note: This cannot be merged with the first iteration (finding group boundaries). This is because we have to
 *            output values for all RowIDs where ANY group by column changes, not only the one we currently iterate
 *            over.
 * 6. Call _aggregate_values for each aggregate, which performs the aggregation and writes the output into
 *    ValueSegments.
 */
std::shared_ptr<const Table> AggregateSort::_on_execute() {
  const auto input_table = left_input_table();

  // Check for invalid aggregates
  _validate_aggregates();

  // Create group by column definitions
  for (const auto& column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id),
                                            input_table->column_is_nullable(column_id));
  }

  // Create aggregate column definitions
  auto aggregate_idx = ColumnID{0};
  for (const auto& aggregate : _aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto column_id = pqp_column.column_id;

    /*
     * Special case for COUNT(*), which is the only case where column equals INVALID_COLUMN_ID:
     * Usually, the data type of the aggregate can depend on the data type of the corresponding input column.
     * For example, the sum of ints is an int, while the sum of doubles is a double.
     * For COUNT(*), the aggregate type is always Long, regardless of the input type.
     * As the input type does not matter and we do not even have an input column,
     * but the function call expects an input type, we choose Long to be consistent with the output type.
     */
    const auto data_type = column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(column_id);

    resolve_data_type(data_type, [&, aggregate_idx](auto type) {
      _create_aggregate_column_definitions(type, aggregate_idx, aggregate->window_function);
    });

    ++aggregate_idx;
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
        if (aggregate->window_function == WindowFunction::Count ||
            aggregate->window_function == WindowFunction::CountDistinct) {
          default_values.emplace_back(int64_t{0});
        } else {
          default_values.emplace_back(NULL_VALUE);
        }
      }
      result_table->append(default_values);
    }

    return result_table;
  }

  auto sorted_table = input_table;
  if (!_groupby_column_ids.empty()) {
    /**
    * If there is a value clustering for a column, it means that all tuples with the same value in that column are in
    * the same chunk. Therefore, if one of the value clustering columns is part of the group by vector, we can skip
    * sorting the whole table and sort on a per-chunk basis instead. Values that would end up in the same cluster are
    * already in the same chunk. In the current definition of value clustering, NULL values must not occur.
    *
    * If the value clustering doesn't match the columns we group by, we need to sort the whole table to achieve the
    * consecutiveness of equal values we need (table-wide sort information is not tracked yet).
    */
    const auto& table_value_clustered_by = input_table->value_clustered_by();
    auto is_value_clustered_by_groupby_column = false;

    if (!table_value_clustered_by.empty()) {
      for (const auto& value_clustered_by : table_value_clustered_by) {
        if (std::find(_groupby_column_ids.begin(), _groupby_column_ids.end(), value_clustered_by) !=
            _groupby_column_ids.end()) {
          is_value_clustered_by_groupby_column = true;
          break;
        }
      }
    }

    if (is_value_clustered_by_groupby_column) {
      // Sort input table chunk-wise as the group by values are clustered.
      sorted_table = _sort_table_chunk_wise(input_table, _groupby_column_ids);
    } else {
      sorted_table = sort_table_by_column_ids(input_table, _groupby_column_ids);
    }
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
  auto group_boundaries = std::set<RowID>{};
  const auto chunk_count = sorted_table->chunk_count();
  for (const auto& column_id : _groupby_column_ids) {
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto previous_value = std::optional<ColumnDataType>{};

      /*
       * Initialize previous_value to the first value in the table, so we avoid considering it a value change.
       * We do not want to consider it as a value change, because we the first row should not be part of the boundaries.
       * For the reasoning behind it see above.
       * We are aware that operator[] is slow, however, for one value it should be faster than segment_iterate_filtered.
       */
      const auto& first_segment = sorted_table->get_chunk(ChunkID{0})->get_segment(column_id);
      const auto& first_value = (*first_segment)[ChunkOffset{0}];
      if (variant_is_null(first_value)) {
        previous_value.reset();
      } else {
        previous_value = boost::get<ColumnDataType>(first_value);
      }

      // Iterate over all chunks and insert RowIDs when values change
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = sorted_table->get_chunk(chunk_id);
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
        const auto& segment = chunk->get_segment(column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (previous_value.has_value() == position.is_null() ||
              (previous_value && !position.is_null() && position.value() != *previous_value)) {
            group_boundaries.insert(RowID{chunk_id, position.chunk_offset()});
            if (position.is_null()) {
              previous_value.reset();
            } else {
              previous_value.emplace(position.value());
            }
          }
        });
      }
    });
  }

  /*
   * For all group by columns
   *   For each group by column
   *     output the column value at the start of the group (it is per definition the same in the whole group)
   * Write outputted values into the result table
   */
  const auto write_groupby_column = [&](const ColumnID input_column_id, const ColumnID output_column_id) {
    const auto column_is_nullable = _output_column_definitions.at(output_column_id).nullable;
    auto group_boundary_iter = group_boundaries.cbegin();
    auto data_type = input_table->column_data_type(input_column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto values = pmr_vector<ColumnDataType>(group_boundaries.size() + 1);
      auto null_values = pmr_vector<bool>(column_is_nullable ? group_boundaries.size() + 1 : 0);

      const auto value_count = values.size();
      for (auto value_index = size_t{0}; value_index < value_count; ++value_index) {
        RowID group_start;
        if (value_index == 0) {
          // First group starts in the first row, but there is no corresponding entry in the set. See above for reasons.
          group_start = RowID{ChunkID{0}, ChunkOffset{0}};
        } else {
          group_start = *group_boundary_iter;
          group_boundary_iter++;
        }

        const auto chunk = sorted_table->get_chunk(group_start.chunk_id);
        const auto& segment = chunk->get_segment(input_column_id);

        /*
         * We are aware that operator[] and AllTypeVariant are known to be inefficient.
         * However, for accessing a single value it is probably more efficient than segment_iterate_filtered,
         * besides being more readable.
         * We cannot use segment_iterate_filtered with the whole group_boundaries (converted to a PosList).
         * This is because the RowIDs in group_boundaries can reference multiple chunks.
         */
        const auto& value = (*segment)[group_start.chunk_offset];

        DebugAssert(!variant_is_null(value) || column_is_nullable, "Null values found in non-nullable column");
        if (variant_is_null(value)) {
          null_values[value_index] = true;
        } else {
          values[value_index] = boost::get<ColumnDataType>(value);
        }
      }

      // Write group by segments
      if (column_is_nullable) {
        _output_segments[output_column_id] =
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
      } else {
        _output_segments[output_column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }
    });
  };

  auto groupby_output_column_id = ColumnID{0};
  for (const auto& input_column_id : _groupby_column_ids) {
    write_groupby_column(input_column_id, groupby_output_column_id);
    ++groupby_output_column_id;
  }

  // Call _aggregate_values for each aggregate
  uint64_t aggregate_index = 0;
  for (const auto& aggregate : _aggregates) {
    /*
     * Special case for COUNT(*), which is the only case where aggregate.column equals INVALID_COLUMN_ID:
     * Usually, the data type of the aggregate can depend on the data type of the corresponding input column.
     * For example, the sum of ints is an int, while the sum of doubles is an double.
     * For COUNT(*), the aggregate type is always an integral type, regardless of the input type.
     * As the input type does not matter and we do not even have an input column,
     * but the function call expects an input type, we choose Int arbitrarily.
     * This is NOT the result type of COUNT(*), which is Long.
     */
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      /*
       * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
       * The problem we encountered: We cannot simply hand aggregate->window_function into the call of _aggregate_values.
       * The reason: the compiler wants to know at compile time which of the templated versions need to be called.
       * However, aggregate->window_function is something that is only available at runtime,
       * so the compiler cannot know its value and thus not deduce the correct method call.
       */
      switch (aggregate->window_function) {
        case WindowFunction::Min: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::Min>(group_boundaries, aggregate_index,
                                                                                sorted_table);
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::Max>(group_boundaries, aggregate_index,
                                                                                sorted_table);
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::Sum>(group_boundaries, aggregate_index,
                                                                                sorted_table);
          break;
        }

        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::Avg>(group_boundaries, aggregate_index,
                                                                                sorted_table);
          break;
        }
        case WindowFunction::Count: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::Count>(group_boundaries, aggregate_index,
                                                                                  sorted_table);
          break;
        }
        case WindowFunction::CountDistinct: {
          using AggregateType = typename WindowFunctionTraits<
              ColumnDataType,
              WindowFunction::CountDistinct>::ReturnType;  // NOLINT(whitespace/line_length)
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::CountDistinct>(
              group_boundaries, aggregate_index, sorted_table);
          break;
        }
        case WindowFunction::StandardDeviationSample: {
          using AggregateType =
              typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;
          _aggregate_values<ColumnDataType, AggregateType, WindowFunction::StandardDeviationSample>(
              group_boundaries, aggregate_index, sorted_table);
          break;
        }
        case WindowFunction::Any: {
          write_groupby_column(input_column_id, ColumnID{static_cast<ColumnID::base_type>(aggregate_index +
                                                                                          _groupby_column_ids.size())});
          break;
          case WindowFunction::CumeDist:
          case WindowFunction::DenseRank:
          case WindowFunction::PercentRank:
          case WindowFunction::Rank:
          case WindowFunction::RowNumber:
            Fail("Unsupported aggregate function " + window_function_to_string.left.at(aggregate->window_function));
        }
      }
    });

    aggregate_index++;
  }

  // Append output to result table
  if (_output_segments.at(0)->size() > 0) {
    result_table->append_chunk(_output_segments);
  }

  // We do not set sort information on the output table as we can only guarantee it in certain situations (e.g., when
  // the whole input table needed to be sorted). As this aggregate case is the fall back solution and should be rather
  // be calculated with the hash aggregate, we neglect the sort information for now and keep it simple.
  return result_table;
}

std::shared_ptr<AbstractOperator> AggregateSort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateSort>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateSort::_on_cleanup() {}

template <typename ColumnType>
void AggregateSort::_create_aggregate_column_definitions(boost::hana::basic_type<ColumnType> /*type*/,
                                                         ColumnID column_index, WindowFunction aggregate_function) {
  /*
   * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
   * There is a similar switch statement in _on_execute for calling _aggregate_values.
   * See the comment there for reasoning.
   */
  switch (aggregate_function) {
    case WindowFunction::Min:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Min>(column_index);
      break;
    case WindowFunction::Max:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Max>(column_index);
      break;
    case WindowFunction::Sum:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Sum>(column_index);
      break;
    case WindowFunction::Avg:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Avg>(column_index);
      break;
    case WindowFunction::Count:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Count>(column_index);
      break;
    case WindowFunction::CountDistinct:
      create_aggregate_column_definitions<ColumnType, WindowFunction::CountDistinct>(column_index);
      break;
    case WindowFunction::StandardDeviationSample:
      create_aggregate_column_definitions<ColumnType, WindowFunction::StandardDeviationSample>(column_index);
      break;
    case WindowFunction::Any:
      create_aggregate_column_definitions<ColumnType, WindowFunction::Any>(column_index);
      break;
    case WindowFunction::CumeDist:
    case WindowFunction::DenseRank:
    case WindowFunction::PercentRank:
    case WindowFunction::Rank:
    case WindowFunction::RowNumber:
      Fail("Unsupported aggregate function " + window_function_to_string.left.at(aggregate_function));
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
template <typename ColumnType, WindowFunction aggregate_function>
void AggregateSort::create_aggregate_column_definitions(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  auto result_type = WindowFunctionTraits<ColumnType, aggregate_function>::RESULT_TYPE;

  const auto& aggregate = _aggregates[column_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (result_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    result_type = left_input_table()->column_data_type(input_column_id);
  }

  const auto nullable =
      (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct &&
       aggregate_function != WindowFunction::Any) ||
      (aggregate_function == WindowFunction::Any && left_input_table()->column_is_nullable(input_column_id));
  const auto column_name =
      aggregate->window_function == WindowFunction::Any ? pqp_column.as_column_name() : aggregate->as_column_name();
  _output_column_definitions.emplace_back(column_name, result_type, nullable);
}
}  // namespace hyrise
