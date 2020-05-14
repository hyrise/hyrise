#include "aggregate_sort.hpp"

#include <algorithm>
#include <memory>
#include <vector>

#include "aggregate/aggregate_traits.hpp"
#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/sort.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/segment_iterate.hpp"
#include "table_wrapper.hpp"
#include "types.hpp"

namespace {

using namespace opossum;  // NOLINT

std::shared_ptr<const Table> sort_table_by_column_ids(const std::shared_ptr<const Table>& table_to_sort,
                                                      const std::vector<ColumnID>& column_ids) {
  // Create sort definition vector from group by list for sort operator
  auto sort_definitions = std::vector<SortColumnDefinition>{};
  sort_definitions.reserve(column_ids.size());
  for (const auto& column_id : column_ids) {
    sort_definitions.emplace_back(SortColumnDefinition{column_id, SortMode::Ascending});
  }

  const auto table_wrapper = std::make_shared<TableWrapper>(table_to_sort);
  table_wrapper->execute();

  const auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions);
  sort->execute();

  return sort->get_output();
}

}  // namespace

namespace opossum {

AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& in,
                             const std::vector<std::shared_ptr<AggregateExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

const std::string& AggregateSort::name() const {
  static const auto name = std::string{"AggregateSort"};
  return name;
}

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
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*_aggregates[aggregate_index]->argument());
  const auto input_column_id = pqp_column.column_id;

  auto aggregate_function = AggregateFunctionBuilder<ColumnType, AggregateType, function>().get_aggregate_function();

  // We already know beforehand how many aggregate values (=group-by-combinations) we have to calculate
  const size_t num_groups = group_boundaries.size() + 1;

  // Vectors to store aggregate values (and if they are NULL) for later usage in value segments
  auto aggregate_results = pmr_vector<AggregateType>(num_groups);
  auto aggregate_null_values = pmr_vector<bool>(num_groups);

  // Variables needed for the aggregates. Not all variables are needed for all aggregates

  // Row counts per group, ex- and including null values. Needed for count (<column>/*) and average
  uint64_t value_count = 0u;
  uint64_t value_count_with_null = 0u;

  // All unique values found. Needed for count distinct
  std::unordered_set<ColumnType> unique_values;

  // The number of the current group-by-combination. Used as offset when storing values
  uint64_t aggregate_group_index = 0u;

  const auto chunk_count = sorted_table->chunk_count();

  std::optional<AggregateType> current_primary_aggregate;
  std::vector<AggregateType> current_secondary_aggregates{};
  ChunkID current_chunk_id{0};
  if (function == AggregateFunction::Count && input_column_id == INVALID_COLUMN_ID) {
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
        count += sorted_table->get_chunk(current_group_begin_pointer.chunk_id)->size() -
                 current_group_begin_pointer.chunk_offset;
        for (auto chunk_id = ChunkID{current_group_begin_pointer.chunk_id + 1}; chunk_id < group_boundary.chunk_id;
             chunk_id++) {
          count += sorted_table->get_chunk(chunk_id)->size();
        }
        count += group_boundary.chunk_offset + 1;
        value_count_with_null = count;
      }
      _set_and_write_aggregate_value<AggregateType, function>(
          aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_primary_aggregate,
          current_secondary_aggregates, value_count, value_count_with_null, unique_values.size());
      current_group_begin_pointer = group_boundary;
      aggregate_group_index++;
    }
    // Compute value count with null values for the last iteration
    value_count_with_null = sorted_table->get_chunk(current_group_begin_pointer.chunk_id)->size() -
                            current_group_begin_pointer.chunk_offset;
    for (auto chunk_id = ChunkID{current_group_begin_pointer.chunk_id + 1}; chunk_id < chunk_count; chunk_id++) {
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
          _set_and_write_aggregate_value<AggregateType, function>(
              aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index,
              current_primary_aggregate, current_secondary_aggregates, value_count, value_count_with_null,
              unique_values.size());

          // Reset helper variables
          current_primary_aggregate = std::optional<AggregateType>();
          current_secondary_aggregates = std::vector<AggregateType>{};
          unique_values.clear();
          value_count = 0u;
          value_count_with_null = 0u;

          // Update indexing variables
          aggregate_group_index++;
          group_boundary_iter++;
        }

        // Update helper variables
        if (!position.is_null()) {
          aggregate_function(new_value, current_primary_aggregate, current_secondary_aggregates);
          value_count++;
          if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
            unique_values.insert(new_value);
          } else if constexpr (function == AggregateFunction::Any) {  // NOLINT
            // Gathering the group's first value for ANY() is sufficient
            return;
          }
        }
        value_count_with_null++;
      });
      current_chunk_id++;
    }
  }
  // Aggregate value for the last group was not written yet
  _set_and_write_aggregate_value<AggregateType, function>(
      aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_primary_aggregate,
      current_secondary_aggregates, value_count, value_count_with_null, unique_values.size());

  // Store the aggregate values in a value segment
  _output_segments[aggregate_index + _groupby_column_ids.size()] =
      std::make_shared<ValueSegment<AggregateType>>(std::move(aggregate_results), std::move(aggregate_null_values));
}

/**
 * This is a generic method to add an aggregate value to the current aggregate value vector (<code>aggregate_results</code>).
 * While adding the new value itself is easy, deciding on what exactly the new value is might be not that easy.
 *
 * Sometimes it is simple, e.g. for Min, Max and Sum the value we want to add is directly the <code>current_primary_aggregate</code>.
 * But sometimes it is more complicated, e.g. Avg effectively only calculates a sum, and we manually need to divide the sum by the number of (non-null) elements that contributed to it.
 * Further, Count and CountDistinct do not use <code>current_x_aggregate</code>, instead we pass <code>value_count</code> and <code>value_count_with_null</code> directly to the function.
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
 * @param current_primary_aggregate the value of the aggregate (return value of the aggregate function) - used by all except COUNT (all versions)
 * @param current_secondary_aggregates the value of a supportive aggregate - used by StandardDeviationSample
 * @param value_count the number of non-null values - used by COUNT(<name>), AVG
 * @param value_count_with_null the number of rows  - used by COUNT(*)
 * @param unique_value_count the number of unique values
 */
template <typename AggregateType, AggregateFunction function>
void AggregateSort::_set_and_write_aggregate_value(
    pmr_vector<AggregateType>& aggregate_results, pmr_vector<bool>& aggregate_null_values,
    const uint64_t aggregate_group_index, [[maybe_unused]] const uint64_t aggregate_index,
    std::optional<AggregateType>& current_primary_aggregate, std::vector<AggregateType>& current_secondary_aggregates,
    [[maybe_unused]] const uint64_t value_count, [[maybe_unused]] const uint64_t value_count_with_null,
    [[maybe_unused]] const uint64_t unique_value_count) const {
  if constexpr (function == AggregateFunction::Count) {  // NOLINT
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*this->_aggregates[aggregate_index]->argument());
    const auto input_column_id = pqp_column.column_id;

    if (input_column_id != INVALID_COLUMN_ID) {
      // COUNT(<name>), so exclude null values
      current_primary_aggregate = value_count;
    } else {
      // COUNT(*), so include null values
      current_primary_aggregate = value_count_with_null;
    }
  }
  if constexpr (function == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>) {  // NOLINT
    // this ignores the case of Avg on strings, but we check in _on_execute() this does not happen

    if (value_count == 0) {
      // there are no non-null values, the average itself must be null (otherwise division by 0)
      current_primary_aggregate = std::optional<AggregateType>();
    } else {
      // normal average calculation
      current_primary_aggregate = *current_primary_aggregate / static_cast<AggregateType>(value_count);
    }
  }
  if constexpr (function == AggregateFunction::StandardDeviationSample &&
                std::is_arithmetic_v<AggregateType>) {  // NOLINT
    // this ignores the case of StandardDeviationSample on strings, but we check in _on_execute() this does not happen

    if (value_count <= 1) {
      current_primary_aggregate = std::optional<AggregateType>();
      current_secondary_aggregates = std::vector<AggregateType>{};
    }
  }
  if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
    current_primary_aggregate = unique_value_count;
  }

  // store whether the value is a null value
  aggregate_null_values[aggregate_group_index] = !current_primary_aggregate;
  if (current_primary_aggregate) {
    // only store non-null values
    aggregate_results[aggregate_group_index] = *current_primary_aggregate;
  }
}

Segments AggregateSort::_get_segments_of_chunk(const std::shared_ptr<const Table>& input_table,
                                               const ChunkID chunk_id) {
  Segments segments{};
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
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    if (!input_table->get_chunk(chunk_id)) continue;

    const auto chunk = std::make_shared<Chunk>(_get_segments_of_chunk(input_table, chunk_id));
    // const auto& chunk = input_table->get_chunk(chunk_id);  // TODO: why not?
    auto single_chunk_to_sort_as_vector = std::vector{chunk};
    auto single_chunk_table = std::make_shared<const Table>(input_table->column_definitions(), input_table->type(),
                                                            std::move(single_chunk_to_sort_as_vector));

    const auto& chunk_sorted_by = input_table->get_chunk(chunk_id)->sorted_by();
    auto output_sorted_by = chunk_sorted_by;

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
        const auto column_count = input_table->column_count();
        const auto pos_list = std::make_shared<EntireChunkPosList>(chunk_id, chunk->size());
        auto reference_segments = Segments{};
        reference_segments.reserve(column_count);

        // Create actual ReferenceSegment objects.
        for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
          auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
          reference_segments.push_back(ref_segment_out);
        }

        output_table->append_chunk(reference_segments);
      } else {
        // In case of a reference segment, we can directly forward it.
        output_table->append_chunk(_get_segments_of_chunk(input_table, chunk_id));
      }
    } else {
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
 *    - The input table is now sorted by all group by columns.
 *    - As a result, all rows that fall into the same group are consecutive within that table.
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
  const auto input_table = input_table_left();

  // Check for invalid aggregates
  _validate_aggregates();

  // Create group by column definitions
  for (const auto& column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id),
                                            input_table->column_is_nullable(column_id));
  }

  // Create aggregate column definitions
  ColumnID aggregate_idx{0};
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
      _create_aggregate_column_definitions(type, aggregate_idx, aggregate->aggregate_function);
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
        if (aggregate->aggregate_function == AggregateFunction::Count ||
            aggregate->aggregate_function == AggregateFunction::CountDistinct) {
          default_values.emplace_back(int64_t{0});
        } else {
          default_values.emplace_back(NULL_VALUE);
        }
      }
      result_table->append(default_values);
    }

    return result_table;
  }

  std::shared_ptr<const Table> sorted_table = input_table;
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
  std::set<RowID> group_boundaries;
  const auto chunk_count = sorted_table->chunk_count();
  for (const auto& column_id : _groupby_column_ids) {
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      std::optional<ColumnDataType> previous_value;

      /*
       * Initialize previous_value to the first value in the table, so we avoid considering it a value change.
       * We do not want to consider it as a value change, because we the first row should not be part of the boundaries.
       * For the reasoning behind it see above.
       * We are aware that operator[] is slow, however, for one value it should be faster than segment_iterate_filtered.
       */
      const auto& first_segment = sorted_table->get_chunk(ChunkID{0})->get_segment(column_id);
      const auto& first_value = (*first_segment)[0];
      if (variant_is_null(first_value)) {
        previous_value.reset();
      } else {
        previous_value.emplace(boost::get<ColumnDataType>(first_value));
      }

      // Iterate over all chunks and insert RowIDs when values change
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = sorted_table->get_chunk(chunk_id);
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
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
      auto values = pmr_vector<ColumnDataType>(group_boundaries.size() + 1);
      auto null_values = pmr_vector<bool>(group_boundaries.size() + 1);

      for (size_t value_index = 0; value_index < values.size(); value_index++) {
        RowID group_start;
        if (value_index == 0) {
          // First group starts in the first row, but there is no corresponding entry in the set. See above for reasons.
          group_start = RowID{ChunkID{0}, 0};
        } else {
          group_start = *group_boundary_iter;
          group_boundary_iter++;
        }

        const auto chunk = sorted_table->get_chunk(group_start.chunk_id);
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
          values[value_index] = boost::get<ColumnDataType>(value);
        }
      }

      // Write group by segments
      _output_segments[groupby_index] =
          std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
    });
    groupby_index++;
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
       * The problem we encountered: We cannot simply hand aggregate->aggregate_function into the call of _aggregate_values.
       * The reason: the compiler wants to know at compile time which of the templated versions need to be called.
       * However, aggregate->aggregate_function is something that is only available at runtime,
       * so the compiler cannot know its value and thus not deduce the correct method call.
       */
      switch (aggregate->aggregate_function) {
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
        case AggregateFunction::StandardDeviationSample: {
          using AggregateType =
              typename AggregateTraits<ColumnDataType, AggregateFunction::StandardDeviationSample>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::StandardDeviationSample>(
              group_boundaries, aggregate_index, sorted_table);
          break;
        }
        case AggregateFunction::Any: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Any>::AggregateType;
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Any>(group_boundaries, aggregate_index,
                                                                                   sorted_table);
          break;
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
    case AggregateFunction::StandardDeviationSample:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::StandardDeviationSample>(column_index);
      break;
    case AggregateFunction::Any:
      create_aggregate_column_definitions<ColumnType, AggregateFunction::Any>(column_index);
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
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (aggregate_data_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    aggregate_data_type = input_table_left()->column_data_type(input_column_id);
  }

  constexpr bool NEEDS_NULL = (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct);
  _output_column_definitions.emplace_back(aggregate->as_column_name(), aggregate_data_type, NEEDS_NULL);
}
}  // namespace opossum
