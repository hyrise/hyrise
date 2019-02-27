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
 * Every time we reach the begin of a new group-by-combination,
 * we store the aggregate value for the current (now previous) group.
 *
 * @tparam ColumnType the type of the input column to aggregate on
 * @tparam AggregateType the type of the aggregate (=output column)
 * @tparam function the aggregate function, as type parameter - e.g. AggregateFunction::MIN, AVG, COUNT, ...
 * @param aggregate_group_offsets the row ids where a new combination in the sorted table begins
 * @param aggregate_index determines which aggregate to calculate (from _aggregates)
 * @param aggregate_function the aggregate function - as callable object. This should always be the same as <code>function</code>
 * @param sorted_table the input table, sorted by the group by columns
 */
template <typename ColumnType, typename AggregateType, AggregateFunction function>
void AggregateSort::_aggregate_values(std::set<RowID>& aggregate_group_offsets, uint64_t aggregate_index,
                                      AggregateFunctor<ColumnType, AggregateType> aggregate_function,
                                      const std::shared_ptr<const Table> &sorted_table) {
  // We already know beforehand how many aggregate values (=group-by-combinations) we have to calculate
  const size_t num_groups = aggregate_group_offsets.size() + 1;

  // Vectors to store aggregate values (and if they are NULL) for later usage in value segments
  auto aggregate_results = std::vector<AggregateType>(num_groups);
  auto aggregate_null_values = std::vector<bool>(num_groups, false);

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
    auto previous_group_offset = RowID{ChunkID{0u}, ChunkOffset{0}};
    for (const auto &aggregate_group_offset : aggregate_group_offsets) {
      if (previous_group_offset.chunk_id == aggregate_group_offset.chunk_id) {
        // Group is located within a single chunk
        value_count_with_null = aggregate_group_offset.chunk_offset - previous_group_offset.chunk_offset;
      } else {
        // Group is spread over multiple chunks
        uint64_t count = 0;
        count += chunks[previous_group_offset.chunk_id]->size() - previous_group_offset.chunk_offset;
        for (auto chunk_id = previous_group_offset.chunk_id + 1; chunk_id < aggregate_group_offset.chunk_id;
             chunk_id++) {
          count += chunks[chunk_id]->size();
        }
        count += aggregate_group_offset.chunk_offset + 1;
        value_count_with_null = count;
      }
      _set_and_write_aggregate_value<ColumnType, AggregateType, function>(
          aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
          value_count, value_count_with_null, unique_values);
      previous_group_offset = aggregate_group_offset;
      aggregate_group_index++;
    }
    value_count_with_null = chunks[previous_group_offset.chunk_id]->size() - previous_group_offset.chunk_offset;
    for (size_t chunk_id = previous_group_offset.chunk_id + 1; chunk_id < chunks.size(); chunk_id++) {
      value_count_with_null += chunks[chunk_id]->size();
    }

  } else {
    /*
     * High-level overview of the algorithm:
     *
     * We already know at which RowIDs a new group (=group-by-combination) begins,
     * it is stored in aggregate_group_offsets.
     * The base idea is:
     *
     * Iterate over every value in the aggregate column, and keep track of the current RowID
     *   if (current row id == start of next group)
     *     write aggregate value of the just finished group
     *     reset helper variables, current aggregate value, etc
     *   else
     *     update helper variables
     *
     */
    auto aggregate_group_offset_iter = aggregate_group_offsets.begin();
    const auto column_id = _aggregates[aggregate_index];
    const auto column = column_id.column;
    for (const auto &chunk : chunks) {
      auto segment = chunk->get_segment(*column);
      segment_iterate<ColumnType>(*segment, [&](const auto& position) {
        const auto row_id = RowID{current_chunk_id, position.chunk_offset()};
        const auto is_new_group = !aggregate_group_offsets.empty() && row_id == *aggregate_group_offset_iter;
        auto new_value = position.value();
        if (is_new_group) {
          // New group is starting. Store the aggregate value of the just finished group
          _set_and_write_aggregate_value<ColumnType, AggregateType, function>(
              aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
              value_count, value_count_with_null, unique_values);

          // Reset helper variables
          current_aggregate_value = std::optional<AggregateType>();
          unique_values.clear();
          value_count = 0u;

          // Process the first element of the new group
          // TODO(anybody) figure out if we can merge this with the else block. Probably?
          value_count_with_null = 1u;
          if (!position.is_null()) {
            aggregate_function(new_value, current_aggregate_value);
            value_count = 1u;
            if constexpr (function == AggregateFunction::CountDistinct) { // NOLINT
              unique_values = {new_value};
            }
          }
          aggregate_group_index++;
          aggregate_group_offset_iter++;

        } else {
          // Group has not changed. Update helper variables
          if (!position.is_null()) {
            aggregate_function(new_value, current_aggregate_value);
            value_count++;
            if constexpr (function == AggregateFunction::CountDistinct) { // NOLINT
              unique_values.insert(new_value);
            }
          }
          value_count_with_null++;
        }
      });
      current_chunk_id++;
    }
  }
  // Aggregate value for the last group was not yet written
  _set_and_write_aggregate_value<ColumnType, AggregateType, function>(
      aggregate_results, aggregate_null_values, aggregate_group_index, aggregate_index, current_aggregate_value,
      value_count, value_count_with_null, unique_values);

  // Store the aggregate values in a value segment
  _output_segments[aggregate_index + _groupby_column_ids.size()] =
      std::make_shared<ValueSegment<AggregateType>>(aggregate_results, aggregate_null_values);
}

/**
 * This is a generic method to add an aggregate value to the current aggregate value vector (<code>aggregate_results</code>).
 * While adding the new value itself is easy, deciding on what exactly the new value might be not that easy.
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
 * @tparam ColumnType TODO remove
 * @tparam AggregateType
 * @tparam function the aggregate function - e.g. AggregateFunction::Min, Max, ...
 * @param aggregate_results the vector where the aggregate values should be stored
 * @param aggregate_null_values the vector indicating whether a specific aggregate value is null
 * @param aggregate_group_index the offset to use for <code>aggregate_results</code> and <code>aggregate_null_values</code>
 * @param aggregate_index current aggregate's offset in <code>_aggregates</code>
 * @param current_aggregate_value the value of the aggregate (return value of the aggregate function) - used by all except COUNT (all versions)
 * @param value_count the number of non-null values - used by COUNT(<name>) and AVG
 * @param value_count_with_null the number of rows  - used by COUNT(*)
 * @param unique_values the unique val TODO replace with unique_values_count
 */
template <typename ColumnType, typename AggregateType, AggregateFunction function>
void AggregateSort::_set_and_write_aggregate_value(
    std::vector<AggregateType>& aggregate_results, std::vector<bool>& aggregate_null_values,
    uint64_t aggregate_group_index, [[maybe_unused]] uint64_t aggregate_index,
    std::optional<AggregateType>& current_aggregate_value, [[maybe_unused]] uint64_t value_count,
    [[maybe_unused]] uint64_t value_count_with_null, const std::unordered_set<ColumnType>& unique_values) const {
  if constexpr (function == AggregateFunction::Count) { // NOLINT
    if (this->_aggregates[aggregate_index].column) {
      // COUNT(<name>), so exclude null values
      current_aggregate_value = value_count;
    } else {
      // COUNT(*), so include null values
      current_aggregate_value = value_count_with_null;
    }
  }
  if constexpr (function == AggregateFunction::Avg &&
                std::is_arithmetic_v<AggregateType>) { // NOLINT
    // this ignores the case of Avg on strings, but we check in _on_execute() this does not happen

    if (value_count == 0) {
      // there are no non-null values, the average itself must be null (otherwise division by 0)
      current_aggregate_value = std::optional<AggregateType>();
    } else {
      // normal average calculation
      current_aggregate_value = *current_aggregate_value / value_count;
    }
  }
  if constexpr (function == AggregateFunction::CountDistinct) { // NOLINT
    current_aggregate_value = unique_values.size();
  }

  // check if the value is a null value
  aggregate_null_values[aggregate_group_index] = !(current_aggregate_value.has_value());
  if (current_aggregate_value.has_value()) {
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
 *
 * For each group by column, iterate over all its values,
 *  and insert RowID into a set when the value is different from the previous one.
 *  The result is a (ordered) set, its entries marking the begin of a new group-by-combination.
 *
 * Iterate once again over the group by columns, this time fetching the values of the previously found groups
 *  and write them into a ValueSegment
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
  for (const auto& aggregate : _aggregates) {
    if (!aggregate.column) {
      if (aggregate.function != AggregateFunction::Count) {
        Fail("Aggregate: Asterisk is only valid with COUNT");
      }
    } else {
      DebugAssert(*aggregate.column < input_table->column_count(), "Aggregate column index out of bounds");
      if (input_table->column_data_type(*aggregate.column) == DataType::String &&
          (aggregate.function == AggregateFunction::Sum || aggregate.function == AggregateFunction::Avg)) {
        Fail("Aggregate: Cannot calculate SUM or AVG on string column");
      }
    }
  }

  // Collect group by column definitions
  for (const auto &column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id), true);
  }

  // Collect aggregate column definitions
  ColumnID column_index{0};
  for (const auto& aggregate : _aggregates) {
    const auto column = aggregate.column;

    // Output column for COUNT(*). int is chosen arbitrarily.
    const auto data_type = !column ? DataType::Int : input_table->column_data_type(*column);

    resolve_data_type(
        data_type, [&, column_index](auto type) { _write_aggregate_output(type, column_index, aggregate.function); });

    ++column_index;
  }

  auto result_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);

  // Handle empty input table according to the SQL standard
  // if group by columns exist -> empty result
  // else -> one row with default values
  if (input_table->empty()) {
    if (_groupby_column_ids.empty()) {
      std::vector<AllTypeVariant> default_values;
      for (const auto &aggregate : _aggregates) {
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
   * All other columns are superfluous, as they will not be part of the result table (and thus accessible for later operators).
   *
   * This could lead to performance issues as the Sort operator needs to materialize more data.
   * We believe this to be a minor issue:
   * It seems that in most cases the optimizer puts a projection before the aggregate operator,
   * and in cases where it does not the table size is small.
   * However, we did not benchmark it, so we cannot prove it.
   */


  // Sort input table consecutively by the group by columns (stable sort)
  auto sorted_table = input_table;
  for (const auto &column_id : _groupby_column_ids) {
    const auto sorted_wrapper = std::make_shared<TableWrapper>(sorted_table);
    sorted_wrapper->execute();
    Sort sort = Sort(sorted_wrapper, column_id);
    sort.execute();
    sorted_table = sort.get_output();
  }

  _output_segments.resize(_aggregates.size() + _groupby_column_ids.size());

  /*
   * Find all RowIDs where a value in any group by column differs from its predecessor,
   * as those are exactly the limits of the different groups.
   * We use a ordered set to "merge" the change locations from multiple columns.
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
   * General note: aggregate_group_offsets contains the first entry of a new group,
   *               or in other words: the last entry of a group + 1 row, similar to vector::end()
   *
   *               It is (in _aggregate_values) used as indicator that a group has ended just before this row.
   *               As a result, the start of the very first group is not represented in aggregate_group_offsets,
   *               as there was no group before that ended.
   *               Further, the end of the last group is not represented as well.
   *               This is because no new group starts after it.
   *               So in total, aggregate_group_offsets will contain one element less than there are groups.
   */
  std::set<RowID> aggregate_group_offsets;
  auto chunks = sorted_table->chunks();
  for (const auto &column_id : _groupby_column_ids) {
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      std::optional<ColumnDataType> previous_value;
      ChunkID chunk_id{0};
      for (const auto &chunk : chunks) {
        auto segment = chunk->get_segment(column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (previous_value && position.value() != *previous_value) {
            aggregate_group_offsets.insert(RowID{chunk_id, position.chunk_offset()});
          }
          // TODO(anybody) shouldnt it be sufficient to do this in an else-block?
          previous_value.emplace(position.value());
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
  for (const auto &column_id : _groupby_column_ids) {
    auto aggregate_group_offset_iter = aggregate_group_offsets.begin();
    auto data_type = input_table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      std::vector<ColumnDataType> values;  // TODO(anybody) required capacity is known, related to "use indices" TODO
      std::vector<bool> null_values;
      bool first_value = true;
      ChunkID chunk_id{0};
      for (const auto &chunk : chunks) {
        auto segment = chunk->get_segment(column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          // ToDo access the row ids directly without iterating
          // thoughts: its just a small part, but this should reduce runtime for this loop from O(input) to O(output)
          const auto row_id = RowID{chunk_id, position.chunk_offset()};
          const auto is_new_group = first_value || row_id == *aggregate_group_offset_iter;
          if (is_new_group) {
            // ToDo use indices
            null_values.emplace_back(position.is_null());
            values.emplace_back(position.value());
            if (!first_value) {
              aggregate_group_offset_iter++;
            }
            first_value = false;
          }
        });
        chunk_id++;
      }
      // Write group by segments
      _output_segments[groupby_index] = std::make_shared<ValueSegment<ColumnDataType>>(values, null_values);
    });
    groupby_index++;
  }

  // Call _aggregate_values for each aggregate
  uint64_t aggregate_index = 0;
  for (const auto& aggregate : _aggregates) {
    const auto data_type = !aggregate.column ? DataType::Int : input_table->column_data_type(*aggregate.column);
    resolve_data_type(data_type, [&, aggregate](auto type) {
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
          // TODO(anybody) maybe we could move this line inside aggregate_values?
          // Would make the switch smaller and we propagate all required template parameters anyway
          auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min>()
                                        .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Min>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
          break;
        }
        case AggregateFunction::Max: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Max>::AggregateType;
          auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max>()
                                        .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Max>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
          break;
        }
        case AggregateFunction::Sum: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AggregateType;
          auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>()
                                        .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Sum>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
          break;
        }

        case AggregateFunction::Avg: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AggregateType;
          auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg>()
                                        .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Avg>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
          break;
        }
        case AggregateFunction::Count: {
          using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Count>::AggregateType;
          auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count>()
                                        .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Count>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
          break;
        }
        case AggregateFunction::CountDistinct: {
          using AggregateType =
              typename AggregateTraits<ColumnDataType, AggregateFunction::CountDistinct>::AggregateType;
          auto aggregate_function =
              AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>()
                  .get_aggregate_function();
          _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>(
              aggregate_group_offsets, aggregate_index, aggregate_function, sorted_table);
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
void AggregateSort::_write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                                            AggregateFunction function) {
  /*
   * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
   * The problem we encountered: We cannot simply hand aggregate.function into the call of _aggregate_values.
   * The reason: the compiler wants to know at compile time which of the templated versions need to be called.
   * However, aggregate.function is something that is only available at runtime,
   * so the compiler cannot know its value and thus not deduce the correct method call.
   */
  switch (function) {
    case AggregateFunction::Min:
      write_aggregate_output<ColumnType, AggregateFunction::Min>(column_index);
      break;
    case AggregateFunction::Max:
      write_aggregate_output<ColumnType, AggregateFunction::Max>(column_index);
      break;
    case AggregateFunction::Sum:
      write_aggregate_output<ColumnType, AggregateFunction::Sum>(column_index);
      break;
    case AggregateFunction::Avg:
      write_aggregate_output<ColumnType, AggregateFunction::Avg>(column_index);
      break;
    case AggregateFunction::Count:
      write_aggregate_output<ColumnType, AggregateFunction::Count>(column_index);
      break;
    case AggregateFunction::CountDistinct:
      write_aggregate_output<ColumnType, AggregateFunction::CountDistinct>(column_index);
      break;
  }
}

template <typename ColumnType, AggregateFunction function>
void AggregateSort::write_aggregate_output(ColumnID column_index) {
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
    column_name_stream << aggregate_function_to_string.left.at(aggregate.function) << "(";
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

