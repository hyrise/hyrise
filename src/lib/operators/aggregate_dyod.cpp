#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

// TODO: Handle strings
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
AggregateType _aggregate_all_values(const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto accumulator = AggregateType{};
  auto value_count = size_t{0};
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      // TODO: handle null values differently for operators that include them like COUNT()??
      if (position.is_null()) {
        return;
      }
      aggregate_function(position.value(), value_count, accumulator);
      ++value_count;
    });
  }

  // Finalize aggregates that cannot be computed incrementally per row.
  if constexpr (window_function == WindowFunction::Count) {
    // The COUNT lambda is a no-op; the actual count is the number of non-NULL values seen.
    return static_cast<AggregateType>(value_count);
  } else if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    // AVG reuses the SUM accumulator and must be divided by the number of contributing values.
    return value_count == 0 ? AggregateType{} : accumulator / static_cast<AggregateType>(value_count);
  } else {
    return accumulator;
  }
}

// Hash for a group key (the concatenated group-by column values of a row). There is no std::hash for
// std::vector<AllTypeVariant>, so we combine the per-value hashes ourselves.
struct GroupKeyHash {
  size_t operator()(const std::vector<AllTypeVariant>& key) const {
    auto seed = size_t{0};
    for (const auto& value : key) {
      boost::hash_combine(seed, std::hash<AllTypeVariant>{}(value));
    }
    return seed;
  }
};

// TODO: Handle strings
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::shared_ptr<std::unordered_map<std::vector<AllTypeVariant>, AggregateType, GroupKeyHash>>
_aggregate_all_values_with_group_by(const std::vector<ColumnID>& groupby_column_ids,
                                    const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  DebugAssert(!groupby_column_ids.empty(), "No group by columns provided");

  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  // auto accumulator = AggregateType{}; //one per group
  // auto value_count = size_t{0};
  const auto chunk_count = input_table->chunk_count();

  // We have a group by with aggregates
  using GroupKeyType = std::vector<
      AllTypeVariant>;  //just append all group_colums values into a vector<AllTypeVariant> and hash that vector?
  const auto value_count_per_group = std::make_shared<std::unordered_map<GroupKeyType, size_t, GroupKeyHash>>();
  const auto aggregates_per_group = std::make_shared<std::unordered_map<GroupKeyType, AggregateType, GroupKeyHash>>();

  //same as before but have the aggregate function acts on aggregates_per_group[position->value()] (using the aggregator?)
  //group by column_ids

  const auto group_by_column_count = groupby_column_ids.size();
  auto group_values_vector = GroupKeyType(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    //get references for all required columns segments
    auto group_segments = std::vector<std::shared_ptr<const AbstractSegment>>(group_by_column_count);
    for (auto group_columnID_index = uint32_t{0}; group_columnID_index < group_by_column_count;
         ++group_columnID_index) {
      const auto columnID = groupby_column_ids[group_columnID_index];
      group_segments[group_columnID_index] = chunk->get_segment(columnID);
    }

    //iterate over all groupby_columns and the input_column_id in lock-step
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    const auto segment_size = aggregate_segment->size();
    //problem with segment iterate is that we need to iterate them in parallel.
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      //get vector for the hash of the groupby columns for this row
      for (auto group_columnID_index = uint32_t{0}; group_columnID_index < group_by_column_count;
           ++group_columnID_index) {
        const auto columnID = groupby_column_ids[group_columnID_index];
        const auto group_by_value = chunk->get_segment(columnID)->operator[](chunk_offset);
        group_values_vector[group_columnID_index] = group_by_value;
      }
      //aggregate into the corresponding group
      const auto value = aggregate_segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        continue;
      }
      const auto typed_value = boost::get<ColumnDataType>(value);
      auto accumulator = aggregates_per_group->operator[](group_values_vector);
      auto value_count = value_count_per_group->operator[](group_values_vector);
      aggregate_function(typed_value, value_count, accumulator);
      aggregates_per_group->operator[](group_values_vector) = accumulator;
      value_count_per_group->operator[](group_values_vector) = ++value_count;
    }
  }

  // Finalize aggregates that cannot be computed incrementally per row.
  if constexpr (window_function == WindowFunction::Count) {
    // The COUNT lambda is a no-op; the actual count lives in `value_count_per_group`.
    for (const auto& [group_key, group_value_count] : *value_count_per_group) {
      aggregates_per_group->operator[](group_key) = static_cast<AggregateType>(group_value_count);
    }
  } else if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    // AVG reuses the SUM accumulator and must be divided by the number of contributing values. AVG on
    // non-arithmetic types (e.g. strings) is rejected elsewhere, so the division is only valid here.
    for (auto& [group_key, accumulator] : *aggregates_per_group) {
      accumulator /= static_cast<AggregateType>(value_count_per_group->at(group_key));
    }
  }

  return aggregates_per_group;
}

template <typename ColumnDataType, typename AggregateType>
void _create_groupby_segment(
    const std::vector<ColumnID>& groupby_column_ids, const std::shared_ptr<const Table>& input_table,
    const std::shared_ptr<std::unordered_map<std::vector<AllTypeVariant>, AggregateType, GroupKeyHash>>&
        aggregates_per_group,
    pmr_vector<std::shared_ptr<AbstractSegment>>& output_segments) {
  const auto groupby_column_count = groupby_column_ids.size();

  for (auto group_by_index = uint32_t{0}; group_by_index < groupby_column_count; ++group_by_index) {
    const auto column_id = groupby_column_ids[group_by_index];
    resolve_data_type(input_table->column_data_type(column_id), [&](const auto groupby_column_data_type_t) {
      using GroupKeyDataType = typename decltype(groupby_column_data_type_t)::type;
      auto result_vector = pmr_vector<GroupKeyDataType>{};
      for (const auto& [group_key, _] : *aggregates_per_group) {
        const auto& group_key_value = group_key[group_by_index];
        const auto typed_group_key_value = boost::get<GroupKeyDataType>(group_key_value);
        result_vector.emplace_back(typed_group_key_value);
      }
      output_segments.emplace_back(std::make_shared<ValueSegment<GroupKeyDataType>>(std::move(result_vector)));
    });
  }
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  const auto input_table = _left_input->get_output();

  _validate_aggregates();

  if (_groupby_column_ids.empty()) {
    // Produces only a single per aggregate.

    const auto aggregate_count = _aggregates.size();
    const auto chunk_count = input_table->chunk_count();

    auto column_definitions = TableColumnDefinitions{};
    auto result_values = std::vector<AllTypeVariant>{};
    column_definitions.reserve(aggregate_count);
    result_values.reserve(aggregate_count);

    for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      const auto& aggregate = _aggregates[aggregate_id];
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);

      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;

      const auto data_type =
          input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
      resolve_data_type(data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        switch (aggregate->window_function) {
          case WindowFunction::Min: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Min>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Max: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Max>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Sum: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Sum>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Avg: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Avg>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Count: {
            // Special case for COUNT(*), simply sum the number of rows, ignoring the input column id.
            // use WindowFunctionExpression.IsCountStar(...) to do this early?
            if (input_column_id == INVALID_COLUMN_ID) {
              auto value_count = size_t{0};
              const auto chunk_count = input_table->chunk_count();
              for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
                const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
                value_count += segment->size();
              }
              result_values.emplace_back(static_cast<int64_t>(value_count));
              break;
            } else {
              using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
              const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Count>(
                  input_table, input_column_id);
              result_values.emplace_back(result);
              break;
            }
          }
          case WindowFunction::CountDistinct: {
            using AggregateType =
                typename WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::ReturnType;
            const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::CountDistinct>(
                input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          // case WindowFunction::StandardDeviationSample: {
          //   using AggregateType =
          //       typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;
          //   const auto result =
          //       _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::StandardDeviationSample>(
          //           input_table, input_column_id);
          //   result_values.emplace_back(result);

          //   break;
          // }
          // TODO:
          // case WindowFunction::Any: {
          //   using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::ReturnType;
          //   const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Any>(input_table, input_column_id);
          //   break;
          // }
          default:
            Fail(std::format("Unsupported aggregate function '{}'.",
                             window_function_to_string.left.at(aggregate->window_function)));
        }
      });
    }

    auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
    result_table->append(result_values);
    return result_table;
  }

  //same as before but have the aggregate function acts on aggregates_per_group[position->value()] (using the aggregator?)
  //group by column_ids

  //1. materialize table into row format because we need to hash all column_ids
  //For a rowID we hash the groupby_columns using a std::vector. We aggregate into a unordered_map with vector keys. At this point we later need the AggregateType but for now use AllTypeVariants.

  // Produces only a single per aggregate.

  const auto aggregate_count = _aggregates.size();
  const auto chunk_count = input_table->chunk_count();

  const auto groupby_column_count = _groupby_column_ids.size();
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_count + aggregate_count);
  auto output_segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  output_segments.reserve(groupby_column_count + aggregate_count);

  // The output schema is [group-by columns..., aggregate columns...]. The group-by segments are produced
  // by `_create_groupby_segment` (during the first aggregate), so their definitions must come first.
  for (const auto groupby_column_id : _groupby_column_ids) {
    column_definitions.emplace_back(input_table->column_name(groupby_column_id),
                                    input_table->column_data_type(groupby_column_id),
                                    input_table->column_is_nullable(groupby_column_id));
  }

  for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    // COUNT and COUNT DISTINCT never produce NULL; all other aggregates can.
    const auto is_count = aggregate->window_function == WindowFunction::Count ||
                          aggregate->window_function == WindowFunction::CountDistinct;
    column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), !is_count);

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      switch (aggregate->window_function) {
        case WindowFunction::Min: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
          const auto result = _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::Min>(
              _groupby_column_ids, input_table, input_column_id);

          if (aggregate_id == 0) {
            _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                   output_segments);
          }
          // Convert result to a ValueSegment
          auto result_vector = pmr_vector<AggregateType>{};
          result_vector.reserve(result->size());
          for (const auto& [_, aggregate_value] : *result) {
            result_vector.emplace_back(aggregate_value);
          }

          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          const auto result = _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::Max>(
              _groupby_column_ids, input_table, input_column_id);
          if (aggregate_id == 0) {
            _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                   output_segments);
          }

          // Convert result to a ValueSegment
          auto result_vector = pmr_vector<AggregateType>{};
          result_vector.reserve(result->size());
          for (const auto& [_, aggregate_value] : *result) {
            result_vector.emplace_back(aggregate_value);
          }

          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          const auto result = _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::Sum>(
              _groupby_column_ids, input_table, input_column_id);
          if (aggregate_id == 0) {
            _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                   output_segments);
          }
          // Convert result to a ValueSegment
          auto result_vector = pmr_vector<AggregateType>{};
          result_vector.reserve(result->size());
          for (const auto& [_, aggregate_value] : *result) {
            result_vector.emplace_back(aggregate_value);
          }

          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
          break;
        }
        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          const auto result = _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::Avg>(
              _groupby_column_ids, input_table, input_column_id);
          if (aggregate_id == 0) {
            _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                   output_segments);
          }
          // Convert result to a ValueSegment
          auto result_vector = pmr_vector<AggregateType>{};
          result_vector.reserve(result->size());
          for (const auto& [_, aggregate_value] : *result) {
            result_vector.emplace_back(aggregate_value);
          }

          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
          break;
        }
        case WindowFunction::Count: {
          // Special case for COUNT(*), simply sum the number of rows, ignoring the input column id.
          // use WindowFunctionExpression.IsCountStar(...) to do this early?
          if (input_column_id == INVALID_COLUMN_ID) {
            // TODO: special case for COUNT(*) :(
            break;
          } else {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
            const auto result =
                _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::Count>(
                    _groupby_column_ids, input_table, input_column_id);
            if (aggregate_id == 0) {
              _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                     output_segments);
            }
            // Convert result to a ValueSegment
            auto result_vector = pmr_vector<AggregateType>{};
            result_vector.reserve(result->size());
            for (const auto& [_, aggregate_value] : *result) {
              result_vector.emplace_back(aggregate_value);
            }

            output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
            break;
          }
        }
        case WindowFunction::CountDistinct: {
          using AggregateType =
              typename WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::ReturnType;
          const auto result =
              _aggregate_all_values_with_group_by<ColumnDataType, AggregateType, WindowFunction::CountDistinct>(
                  _groupby_column_ids, input_table, input_column_id);
          if (aggregate_id == 0) {
            _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
                                                                   output_segments);
          }
          // Convert result to a ValueSegment
          auto result_vector = pmr_vector<AggregateType>{};
          result_vector.reserve(result->size());
          for (const auto& [_, aggregate_value] : *result) {
            result_vector.emplace_back(aggregate_value);
          }

          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result_vector)));
          break;
        }
        // case WindowFunction::StandardDeviationSample: {
        //   using AggregateType =
        //       typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;
        //   const auto result = _aggregate_all_values_with_group_by<ColumnDataType, AggregateType,
        //                                                           WindowFunction::StandardDeviationSample>(
        //       _groupby_column_ids, input_table, input_column_id);
        //   if (aggregate_id == 0) {
        //     _create_groupby_segment<ColumnDataType, AggregateType>(_groupby_column_ids, input_table, result,
        //                                                            output_segments);
        //   }
        //   output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result)));

        //   break;
        // }
        // TODO:
        // case WindowFunction::Any:
        default:
          Fail(std::format("Unsupported aggregate function '{}'.",
                           window_function_to_string.left.at(aggregate->window_function)));
      }
    });
  }

  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  result_table->append_chunk(output_segments);

  // return std::make_shared<Table>(TableColumnDefinitions{{"dummy", DataType::Int, false}}, TableType::Data);
  return result_table;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
