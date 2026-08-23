#include "aggregate_dyod.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <compare>
#include <cstdint>
#include <cstring>
#include <format>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>

#include "uninitialized_vector.hpp"

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "expression/window_function_expression.hpp"
#include "utils/assert.hpp"
#include "storage/chunk.hpp"
#include "resolve_type.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

template <typename ColumnDataType, typename AggregateType>
class WindowFunctionBuilder<ColumnDataType, AggregateType, WindowFunction::Any> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const size_t /*aggregate_count*/, AggregateType& accumulator) {
      accumulator = new_value;
    };
  }
};

namespace {

template <typename Functor>
void resolve_window_function(const WindowFunction window_function, const Functor& functor) {
  switch (window_function) {
    case WindowFunction::Min:
      functor(std::integral_constant<WindowFunction, WindowFunction::Min>{});
      return;
    case WindowFunction::Max:
      functor(std::integral_constant<WindowFunction, WindowFunction::Max>{});
      return;
    case WindowFunction::Sum:
      functor(std::integral_constant<WindowFunction, WindowFunction::Sum>{});
      return;
    case WindowFunction::Avg:
      functor(std::integral_constant<WindowFunction, WindowFunction::Avg>{});
      return;
    case WindowFunction::Count:
      functor(std::integral_constant<WindowFunction, WindowFunction::Count>{});
      return;
    case WindowFunction::CountDistinct:
      functor(std::integral_constant<WindowFunction, WindowFunction::CountDistinct>{});
      return;
    case WindowFunction::Any:
      functor(std::integral_constant<WindowFunction, WindowFunction::Any>{});
      return;
    case WindowFunction::StandardDeviationSample:
      functor(std::integral_constant<WindowFunction, WindowFunction::StandardDeviationSample>{});
      return;
    default:
      Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
  }
}

template <typename T>
concept arithmetic = std::integral<T> || std::floating_point<T>;

// We normalize the input value, which is required to be an arithmetic data type
// by copying the byte representation into the key buffer. We then add an extra byte
// to signal whether the value is null. Since we write sizeof(value) bytes for the normalized
// value and reserve this size + 1, the address in the last line will not be out of bounds.
template <arithmetic DataType>
void normalize_numerical(const DataType value, const bool is_null, uint8_t* key_buffer) {
  const auto* const bytes = reinterpret_cast<const std::uint8_t*>(&value);
  std::memcpy(key_buffer, bytes, sizeof(value));
  *(key_buffer + sizeof(value)) = is_null ? 0 : 255;
}

const auto max_string_key_length = uint64_t{8};

void normalize_string(const pmr_string& value, const bool is_null, uint8_t* key_buffer) {
  const auto write_length = std::min(max_string_key_length, uint64_t{value.length()});
  // Since the string is never read as such we do not have to ensure null termination.
  std::memcpy(key_buffer, value.data(), write_length);  // NOLINT(bugprone-not-null-terminated-result)
  std::memset(key_buffer + write_length, 0, max_string_key_length - write_length);
  *(key_buffer + max_string_key_length) = is_null ? 0 : 255;
}

uint8_t key_data_length(const DataType data_type) {
  return (data_type == DataType::Int || data_type == DataType::Float) ? 4 : 8;
}

uint8_t key_data_length_null(const DataType data_type) {
  return key_data_length(data_type) + 1;
}

}  // namespace

void AggregateDYOD::_normalize_chunk_groupby(const std::shared_ptr<const Chunk>& input_chunk,
                                             const uint64_t row_offset,
                                             uninitialized_vector<AggregateDYOD::NormalizedKey>& key_vector,
                                             uninitialized_vector<uint8_t>& byte_vector,
                                             pmr_vector<pmr_string>& groupby_strings) {
  auto groupby_string_index = uint64_t{0};
  const auto chunk_size = input_chunk->size();
  auto byte_offset = uint64_t{0};

  for (const auto column_id : groupby_column_ids()) {
    const auto column_data_type = left_input_table()->column_data_type(column_id);
    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto segment = input_chunk->get_segment(column_id);
      auto row_id = uint64_t{0};
      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        const auto byte_index =
            ((row_offset + row_id) * (_normalized_key_size + _groupby_string_count * 8)) + byte_offset;
        auto* base_byte = byte_vector.data() + byte_index - byte_offset;
        auto* byte_representation = byte_vector.data() + byte_index;
        if constexpr (std::is_arithmetic_v<ColumnDataType>) {
          const auto value = position.is_null() ? ColumnDataType{0} : position.value();
          normalize_numerical<ColumnDataType>(value, position.is_null(), byte_representation);
        } else if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          // For strings we additionally need to store the whole string value in the groupy_strings vector
          const auto value = position.is_null() ? "" : position.value();
          normalize_string(value, position.is_null(), byte_representation);
          const auto string_index = (row_offset * _groupby_string_count) + (groupby_string_index * chunk_size) + row_id;
          // We store the index to the string at the back of the key, which we do not use for comparisons
          std::memcpy(base_byte + _normalized_key_size + (groupby_string_index * 8), &string_index,
                      sizeof(string_index));
          groupby_strings[string_index] = value;
        } else {
          Fail("DataType is not supported by Hyrise.");
        }
        key_vector[row_offset + row_id] = (row_offset + row_id) * (_normalized_key_size + _groupby_string_count * 8);
        ++row_id;
      });
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        ++groupby_string_index;
      }
    });
    byte_offset += key_data_length_null(column_data_type);
  }
}

void AggregateDYOD::_materialize_chunk_aggregates(const std::shared_ptr<const Chunk>& input_chunk,
                                                  const uint64_t row_offset) {
  const auto column_count = _unique_aggregate_columns.size();
  for (auto column_position = uint64_t{0}; column_position < column_count; ++column_position) {
    const auto column_id = _unique_aggregate_columns[column_position];

    resolve_data_type(left_input_table()->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto materialized_column = std::static_pointer_cast<MaterializedColumn<ColumnDataType>>(
          _materialized_aggregate_columns[column_position]);

      auto* const values = materialized_column->values.data() + row_offset;
      auto* const null_values = materialized_column->null_values.data() + row_offset;

      auto row_index = uint64_t{0};
      segment_iterate<ColumnDataType>(*input_chunk->get_segment(column_id), [&](const auto& position) {
        values[row_index] = position.is_null() ? ColumnDataType{} : position.value();
        null_values[row_index] = position.is_null();
        ++row_index;
      });
    });
  }
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  using Morsel = AggregateDYOD::Morsel;

  _validate_aggregates();

  _groupby_string_count = std::ranges::count_if(_groupby_column_ids, [&](auto column_id) {
    return left_input_table()->column_data_type(column_id) == DataType::String;
  });

  const auto input_table = left_input_table();
  const auto row_count = input_table->row_count();

  // Create group column definitions and calculate size of normalized keys
  for (const auto& column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id),
                                            input_table->column_is_nullable(column_id));

    const auto data_type = left_input_table()->column_data_type(column_id);
    _normalized_key_size += key_data_length(data_type);
    _normalized_key_size += 1;
  }

  // Create aggregate column definitions
  auto aggregate_index = ColumnID{0};
  auto aggregate_column_offset = uint64_t{0};

  for (const auto& aggregate : _aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto column_id = pqp_column.column_id;

    // We determine the set of unique columns that are needed for aggregation as to not
    // materialize the same columns multiple times
    if (column_id != INVALID_COLUMN_ID && !_aggregate_column_position.contains(column_id)) {
      _unique_aggregate_columns.push_back(column_id);
      _aggregate_column_position[column_id] = aggregate_column_offset++;
    }
    /*
     * Special case for COUNT(*), which is the only case where column equals INVALID_COLUMN_ID:
     * Usually, the data type of the aggregate can depend on the data type of the corresponding input column.
     * For example, the sum of ints is an int, while the sum of doubles is a double.
     * For COUNT(*), the aggregate type is always Long, regardless of the input type.
     * As the input type does not matter and we do not even have an input column,
     * but the function call expects an input type, we choose Long to be consistent with the output type.
     */
    const auto data_type = column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(column_id);

    resolve_data_type(data_type, [&, aggregate_index](auto type) {
      create_aggregate_column_definitions(type, aggregate_index, aggregate->window_function);
    });

    ++aggregate_index;
  }

  // Create a MaterializedColumn of the concrete type for each column some aggregate refers to
  const auto num_unique_aggregate_columns = _unique_aggregate_columns.size();
  _materialized_aggregate_columns.resize(num_unique_aggregate_columns);
  for (auto position = uint64_t{0}; position < num_unique_aggregate_columns; ++position) {
    const auto column_id = _unique_aggregate_columns[position];
    resolve_data_type(input_table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      _materialized_aggregate_columns[position] = std::make_shared<MaterializedColumn<ColumnDataType>>(row_count);
    });
  }

  auto result_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);

  /*
   * Handle empty input table according to the SQL standard
   *  if group by columns exist -> empty result
   *  else -> one row with default values
   */
  if (input_table->empty()) {
    if (_groupby_column_ids.empty()) {
      std::vector<AllTypeVariant> default_values;
      default_values.reserve(_aggregates.size());
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

  /*
   * We distinguish two cases:
   *  1. There are no columns to group by:
   *    In this case the aggregation only needs to iterate over each column and aggregate with the respective function,
   *    resulting in a table with a single row
  */
  if (_groupby_column_ids.empty()) {
    auto aggregate_values = pmr_vector<std::shared_ptr<AbstractSegment>>(_aggregates.size());
    aggregate_index = 0;
    for (const auto& aggregate : _aggregates) {
      const auto& pqp_expression = static_cast<const PQPColumnExpression&>(*_aggregates[aggregate_index]->argument());
      const auto pqp_column_id = pqp_expression.column_id;

      const auto data_type =
          pqp_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(pqp_column_id);

      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        resolve_window_function(aggregate->window_function, [&](auto window_function_constant) {
          constexpr auto AGGREGATE_FUNCTION = decltype(window_function_constant)::value;
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, AGGREGATE_FUNCTION>::ReturnType;

          aggregate_values[aggregate_index] =
              _aggregate_values_without_groups<ColumnDataType, AggregateType, AGGREGATE_FUNCTION>(aggregate_index);
        });
      });
      ++aggregate_index;
    }
    result_table->append_chunk(aggregate_values);
    return result_table;
  }
  /*  
   * 2. There are columns to group by:
   *      In this case, we initialize vectors to hold the bytes for NormalizedKeys, pointers to these keys
   *      and all strings in the group-by columns. These need to be stored seperately as we do not want to
   *      have keys of different sizes and such that the different values  of each column are aligned in
   *      every key.
   *      The normalization is done in parallel for every chunk and each thread/task writes to a distinct
   *      range of the vectors.
   *      Now the rows are split into morsels. Then all morsels are queued to perform their aggregations.
   *      When all morsels have finished, we binary merge the morsels together.
   *      We then write the group and aggregate values into the result table and return.
  */
    auto key_bytes = uninitialized_vector<uint8_t>(row_count * (_normalized_key_size + _groupby_string_count * 8));
    auto normalized_keys = uninitialized_vector<NormalizedKey>(row_count);
    auto groupby_strings = pmr_vector<pmr_string>(row_count * _groupby_string_count);

    const auto chunk_count = input_table->chunk_count();
    auto chunk_normalization_tasks = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);

    auto row_offset = uint64_t{0};
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      chunk_normalization_tasks[chunk_id] = std::make_shared<JobTask>([&, chunk_id, row_offset]() {
        const auto chunk = input_table->get_chunk(chunk_id);
        _normalize_chunk_groupby(chunk, row_offset, normalized_keys, key_bytes, groupby_strings);
        _materialize_chunk_aggregates(chunk, row_offset);
      });
      row_offset += input_table->get_chunk(chunk_id)->size();
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(chunk_normalization_tasks);

    const auto is_multi_threaded = Hyrise::get().is_multi_threaded();
    // If we run on a single thread we avoid the overhead of splitting everything up into chunks and
    // still processing them sequentially and perform aggregation over the entire table in one go.
    // This avoids merging as well, which can be quite expensive with our implementation.
    const auto desired_morsel_size = is_multi_threaded ? ChunkOffset{10'000} : row_count;
    const auto morsel_count =
        is_multi_threaded ? (row_count + desired_morsel_size - 1) / desired_morsel_size : 1;

    auto morsels = std::vector<std::shared_ptr<Morsel>>(morsel_count);
    auto aggregation_tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    aggregation_tasks.reserve(morsel_count);
    auto morsel_range_start = uint64_t{0};

    for (auto& morsel : morsels) {
      const auto morsel_range_end = std::min(morsel_range_start + desired_morsel_size, input_table->row_count()) - 1;
      const auto morsel_size = morsel_range_end - morsel_range_start + 1;
      auto normalized_key_span = std::span<NormalizedKey>(normalized_keys.begin() + morsel_range_start, morsel_size);

      morsel = std::make_shared<Morsel>(*this, morsel_size, morsel_range_start, key_bytes, normalized_key_span,
                                        groupby_strings);
      morsel_range_start += desired_morsel_size;

      aggregation_tasks.push_back(std::make_shared<JobTask>([&]() {
        morsel->sort_morsel_range();

        auto aggregate_index = uint64_t{0};
        for (const auto& aggregate : _aggregates) {
          const auto& pqp_expression = static_cast<const PQPColumnExpression&>(*aggregate->argument());
          const auto pqp_column_id = pqp_expression.column_id;

          const auto data_type =
              pqp_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(pqp_column_id);
          resolve_data_type(data_type, [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            resolve_window_function(aggregate->window_function, [&](auto window_function_constant) {
              constexpr auto AGGREGATE_FUNCTION = decltype(window_function_constant)::value;
              using AggregateType = typename WindowFunctionTraits<ColumnDataType, AGGREGATE_FUNCTION>::ReturnType;

              morsel->aggregate_morsel<ColumnDataType, AGGREGATE_FUNCTION, AggregateType>(aggregate_index);
            });
          });
          ++aggregate_index;
        }
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(aggregation_tasks);

    // We merge the morsel results with a binary strategy. For this we start with a merge distance
    // of one, where a morsel is merged with the next. We now double the distance, so a morsel gets merged
    // with one distance two away, and thus contains the merged results of 4 consecutive morsels. We repeat this
    // process until the first morsel contains all information.
    for (auto merge_distance = size_t{1}; merge_distance < morsel_count; merge_distance *= 2) {
      auto merge_tasks = std::vector<std::shared_ptr<AbstractTask>>{};
      merge_tasks.reserve((morsel_count + merge_distance - 1) / (2 * merge_distance));
      for (auto morsel_id = size_t{0}; morsel_id + merge_distance < morsel_count; morsel_id += 2 * merge_distance) {
        merge_tasks.push_back(std::make_shared<JobTask>([&, morsel_id, merge_distance]() {
          const auto& morsel1 = morsels[morsel_id];
          auto morsel2 = morsels[morsel_id + merge_distance];

          morsel1->merge_morsel(morsel2);
        }));
      }

      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(merge_tasks);
    }

    // Our binary merge strategy guarantees that the final results are in the first morsel.
    const auto final_morsel = morsels[0];
    const auto value_count = final_morsel->group_count;

    // Writing the group values to the table
    // Since we store everything except strings directly in its byte representation in the keys
    // we simply decode it from there. For strings we decode the index into the global string
    // buffer and get the string from there.
    auto group_key_offset = uint64_t{0};
    auto group_string_offset = uint64_t{0};
    for (const auto& column_id : _groupby_column_ids) {
      const auto column_is_nullable = left_input_table()->column_is_nullable(column_id);
      const auto data_type = left_input_table()->column_data_type(column_id);
      const auto data_length = key_data_length(data_type);
      auto value_index = uint64_t{0};
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto values = pmr_vector<ColumnDataType>(value_count);
        auto null_values = pmr_vector<bool>(value_count);

        for (const auto& key : final_morsel->group_keys) {
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            std::memcpy(&values[value_index], key_bytes.data() + key + group_key_offset, data_length);
          } else {
            auto string_index = uint64_t{0};
            std::memcpy(&string_index, key_bytes.data() + key + _normalized_key_size + (group_string_offset * 8), 8);
            values[value_index] = groupby_strings[string_index];
          }
          null_values[value_index] = key_bytes[key + group_key_offset + data_length] == 0;
          ++value_index;
        }

        if (column_is_nullable) {
          _output_segments.push_back(
              std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values)));
        } else {
          _output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values)));
        }
        group_key_offset += key_data_length_null(data_type);
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          ++group_string_offset;
        }
      });
    }

    aggregate_index = uint64_t{0};
    for (const auto& aggregate : _aggregates) {
      const auto& pqp_expression = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto pqp_column_id = pqp_expression.column_id;
      const auto data_type =
          pqp_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(pqp_column_id);
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        resolve_window_function(aggregate->window_function, [&](auto window_function_constant) {
          constexpr auto AGGREGATE_FUNCTION = decltype(window_function_constant)::value;
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, AGGREGATE_FUNCTION>::ReturnType;
          const auto nullable =
              (AGGREGATE_FUNCTION != WindowFunction::Count && AGGREGATE_FUNCTION != WindowFunction::CountDistinct &&
               AGGREGATE_FUNCTION != WindowFunction::Any) ||
              (AGGREGATE_FUNCTION == WindowFunction::Any && left_input_table()->column_is_nullable(pqp_column_id));

          // This case is needed here to tell the compiler that to_value_segment will not
          // be instantiated with StandardDeviationSample and a non-arithmetic type.
          if constexpr (AGGREGATE_FUNCTION == WindowFunction::StandardDeviationSample &&
                        !std::is_arithmetic_v<ColumnDataType>) {
            Fail("Standard Deviation sampling is not supported on non-arithmetic types.");
          } else if constexpr (AGGREGATE_FUNCTION == WindowFunction::CountDistinct) {
            _output_segments.push_back(final_morsel->distinct_to_value_segment<ColumnDataType>(aggregate_index));
          } else {
            _output_segments.push_back(
                final_morsel->to_value_segment<AGGREGATE_FUNCTION, AggregateType>(aggregate_index, nullable));
          }
        });
      });
      ++aggregate_index;
    }

    result_table->append_chunk(_output_segments);
    return result_table;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

std::weak_ordering AggregateDYOD::Morsel::compare_keys(const NormalizedKey& first, const NormalizedKey& second) const {
  const auto key_size = morsel_operator._normalized_key_size;
  const auto memcmp_result = memcmp(&key_bytes[first], &key_bytes[second], key_size);

  if (memcmp_result < 0) {
    return std::weak_ordering::less;
  } else if (memcmp_result > 0) {
    return std::weak_ordering::greater;
  }

  for (auto index = uint64_t{0}; index < morsel_operator._groupby_string_count; ++index) {
    auto index1 = uint64_t{0};
    auto index2 = uint64_t{0};
    std::memcpy(&index1, key_bytes.data() + first + key_size + (index * 8), 8);
    std::memcpy(&index2, key_bytes.data() + second + key_size + (index * 8), 8);
    // We use the <=> operator to directly get an ordering of the compared strings.
    const auto strcmp_result = groupby_strings[index1] <=> groupby_strings[index2];
    if (strcmp_result == 0) {
      continue;
    }
    return strcmp_result;
  }

  return std::weak_ordering::equivalent;
}

void AggregateDYOD::Morsel::sort_morsel_range() {
  /*
   * To sort the morsel we sort indices of the rows according to the normalized keys.
   * With these indices we can then reorder the MaterializedColumns for the aggregates
   * without the need to materialize into a row-wise format. We pack the key-index and
   * the row-reference-index into a pair to get more predictable accesses on this level.
   * When accessing the keys themselves, this may still be quite random.
  */
  auto sort_values = std::vector<std::pair<NormalizedKey, uint64_t>>(row_count);
  for (auto index = uint64_t{0}; index < row_count; ++index) {
    auto& [key, sort_index] = sort_values[index];
    key = normalized_keys[index];
    sort_index = index;
  }

  boost::sort::pdqsort(sort_values.begin(), sort_values.end(), [&](auto first_key, auto second_key) {
    return compare_keys(first_key.first, second_key.first) < 0;
  });

  auto sorted_indices = std::vector<uint64_t>();
  sorted_indices.reserve(row_count);
  for (auto index = uint64_t{0}; index < row_count; ++index) {
    auto& [key, sort_index] = sort_values[index];
    normalized_keys[index] = key;
    sorted_indices.push_back(sort_index);
  }

  const auto& unique_aggregate_columns = morsel_operator._unique_aggregate_columns;
  const auto num_unique_aggregate_columns = unique_aggregate_columns.size();
  for (auto position = uint64_t{0}; position < num_unique_aggregate_columns; ++position) {
    const auto column_id = unique_aggregate_columns[position];
    resolve_data_type(morsel_operator.left_input_table()->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto materialized_column = std::static_pointer_cast<MaterializedColumn<ColumnDataType>>(
          morsel_operator._materialized_aggregate_columns[morsel_operator._aggregate_column_position.at(column_id)]);
      auto* values = materialized_column->values.data() + initial_row_offset;
      auto* null_values = materialized_column->null_values.data() + initial_row_offset;

      auto reordered_values = pmr_vector<ColumnDataType>(row_count);
      auto reordered_nulls = uninitialized_vector<uint8_t>(row_count);

      for (auto index = uint64_t{0}; index < row_count; ++index) {
        reordered_values[index] = std::move(values[sorted_indices[index]]);
        reordered_nulls[index] = std::move(null_values[sorted_indices[index]]);
      }

      // Since (pmr_)strings are not TriviallyCopyable we cannot use memcpy for them.
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        std::move(reordered_values.begin(), reordered_values.end(),
                  materialized_column->values.begin() + initial_row_offset);
      } else {
        std::memcpy(values, reordered_values.data(), row_count * sizeof(ColumnDataType));
      }
      std::memcpy(null_values, reordered_nulls.data(), row_count * sizeof(uint8_t));
    });
  }

  // We compare subsequent keys to figure out the group sizes and
  // to get a representative key for each group (which are by definition
  // the same for each group member)
  // Since the keys are already ordered, we know that they will compare
  // either less or equal and we do not have to check for greater.
  auto current_group_size = uint64_t{0};
  for (auto row_index = uint64_t{0}; row_index < row_count - 1; ++row_index) {
    ++current_group_size;
    if (compare_keys(normalized_keys[row_index], normalized_keys[row_index + 1]) != 0) {
      group_sizes.push_back(current_group_size);
      group_keys.push_back(normalized_keys[row_index]);
      current_group_size = 0;
    }
  }
  group_sizes.push_back(current_group_size + 1);
  group_keys.push_back(normalized_keys.back());
  group_count = group_sizes.size();
}

template <typename ColumnType, WindowFunction aggregate_function, typename AggregateType>
void AggregateDYOD::Morsel::aggregate_morsel(const uint64_t aggregate_index) {
  const auto& pqp_column =
      static_cast<const PQPColumnExpression&>(*morsel_operator._aggregates[aggregate_index]->argument());
  const auto input_column_id = pqp_column.column_id;
  auto value_count = uint64_t{0};

  auto accumulator = AggregateAccumulator<aggregate_function, AggregateType>{};
  auto aggregator = WindowFunctionBuilder<ColumnType, AggregateType, aggregate_function>().get_aggregate_function();
  auto distinct_values = std::unordered_set<ColumnType>{};

  using Results = AggregateResults<aggregate_function, AggregateType>;
  using DistinctResults = AggregateResults<WindowFunction::CountDistinct, ColumnType>;

  auto results = std::make_shared<
      std::conditional_t<aggregate_function == WindowFunction::CountDistinct, DistinctResults, Results>>(group_count);

  aggregate_results[aggregate_index] = results;

  auto& accumulators = results->accumulators;
  auto& counts = results->counts;

  // Special COUNT(*) implementation
  // Since we already have the group sizes from the sorting, we can use them to return the counts.
  // We have to split both conditions so that we can make the first
  // constexpr to guarantee the compiler that the AggregateResult
  // will not be templated with WindowFunction::CountDistinct, which requires
  // the DistinctResult.
  if constexpr (aggregate_function == WindowFunction::Count) {
    if (input_column_id == INVALID_COLUMN_ID) {
      auto last_group_offset = uint64_t{0};
      for (auto group_index = uint64_t{0}; group_index < group_count; ++group_index) {
        const auto group_size = group_sizes[group_index];
        const auto next_offset = last_group_offset + group_size;
        accumulators[group_index] = accumulator;
        counts[group_index] = group_size;
        group_keys[group_index] = normalized_keys[next_offset - 1];
        last_group_offset = next_offset;
      }
      return;
    }
  }

  const auto column_index = morsel_operator._aggregate_column_position.at(input_column_id);
  const auto column = std::static_pointer_cast<MaterializedColumn<ColumnType>>(
      morsel_operator._materialized_aggregate_columns[column_index]);
  auto* values = column->values.data() + initial_row_offset;
  auto* null_values = column->null_values.data() + initial_row_offset;

  auto current_group_offset = uint64_t{0};
  for (auto group_index = uint64_t{0}; group_index < group_count; ++group_index) {
    const auto group_size = group_sizes[group_index];
    for (auto row_index = current_group_offset; row_index < current_group_offset + group_size; ++row_index) {
      const auto is_null = null_values[row_index];
      if (is_null) {
        continue;
      }
      auto& value = values[row_index];

      aggregator(value, value_count, accumulator);
      ++value_count;
      if constexpr (aggregate_function == WindowFunction::CountDistinct) {
        distinct_values.insert(value);
      }
    }

    if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      accumulators[group_index] = pmr_vector<ColumnType>(distinct_values.begin(), distinct_values.end());
      counts[group_index] = accumulators[group_index].size();
      group_keys[group_index] = normalized_keys[current_group_offset];
    } else {
      accumulators[group_index] = accumulator;
      counts[group_index] = value_count;
      group_keys[group_index] = normalized_keys[current_group_offset];
    }
    accumulator = AggregateAccumulator<aggregate_function, AggregateType>{};
    distinct_values.clear();
    value_count = 0;
    current_group_offset += group_size;
  }
}

void AggregateDYOD::Morsel::merge_morsel(std::shared_ptr<Morsel>& other) {
  /* 
    * We first create a plan (instructions how to merge the two morsels) to only compare the keys once.
    * According to this plan every aggregate is then merged separately.
    * We then create a new list of representative keys from the same merge plan.
  */

  pmr_vector<MergeStep> merge_plan;
  merge_plan.reserve(group_count + other->group_count);

  auto source_index = uint64_t{0};
  auto other_index = uint64_t{0};
  while (source_index < group_count && other_index < other->group_count) {
    const auto key_compare_result = compare_keys(group_keys[source_index], other->group_keys[other_index]);

    if (key_compare_result < 0) {
      merge_plan.emplace_back(source_index, -1);
      ++source_index;
    } else if (key_compare_result > 0) {
      merge_plan.emplace_back(-1, other_index);
      ++other_index;
    } else {
      merge_plan.emplace_back(source_index, other_index);
      ++source_index;
      ++other_index;
    }
  }
  while (source_index < group_count) {
    merge_plan.emplace_back(source_index, -1);
    ++source_index;
  }
  while (other_index < other->group_count) {
    merge_plan.emplace_back(-1, other_index);
    ++other_index;
  }

  auto aggregate_index = uint64_t{0};
  for (const auto& aggregate : morsel_operator._aggregates) {
    const auto& pqp_expression = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto pqp_column_id = pqp_expression.column_id;

    const auto data_type = pqp_column_id == INVALID_COLUMN_ID
                               ? DataType::Long
                               : morsel_operator.left_input_table()->column_data_type(pqp_column_id);
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto window_function_constant) {
        constexpr auto AGGREGATE_FUNCTION = decltype(window_function_constant)::value;
        using AggregateType = typename WindowFunctionTraits<ColumnDataType, AGGREGATE_FUNCTION>::ReturnType;
        if constexpr (AGGREGATE_FUNCTION == WindowFunction::CountDistinct) {
          merge_single_aggregate<AGGREGATE_FUNCTION, ColumnDataType>(other, aggregate_index, merge_plan);
        } else {
          merge_single_aggregate<AGGREGATE_FUNCTION, AggregateType>(other, aggregate_index, merge_plan);
        }
      });
    });
    ++aggregate_index;
  }
  const auto new_group_count = merge_plan.size();
  auto merged_group_keys = pmr_vector<NormalizedKey>(new_group_count);
  for (auto step_index = uint64_t{0}; step_index < new_group_count; ++step_index) {
    const auto [source_index, other_index] = merge_plan[step_index];
    if (source_index == -1 && other_index > -1) {
      merged_group_keys[step_index] = other->group_keys[other_index];
    } else {
      merged_group_keys[step_index] = group_keys[source_index];
    }
  }

  group_keys = std::move(merged_group_keys);
  group_count = new_group_count;
}

template <WindowFunction aggregate_function, typename AggregateType>
void AggregateDYOD::Morsel::merge_single_aggregate(std::shared_ptr<Morsel>& other, const uint64_t aggregate_index,
                                                    const pmr_vector<MergeStep>& merge_plan) {
  using Results = AggregateResults<aggregate_function, AggregateType>;
  auto aggregator = WindowFunctionBuilder<AggregateType, AggregateType, aggregate_function>().get_aggregate_function();

  const auto new_group_count = merge_plan.size();
  auto merged = std::make_shared<Results>(new_group_count);
  auto source_results = std::static_pointer_cast<Results>(aggregate_results[aggregate_index]);
  auto other_results = std::static_pointer_cast<Results>(other->aggregate_results[aggregate_index]);

  /*
   * According to the plan we either move the accumulator from one morsel or we have to combine the results.
   * This case involves mostly one more aggregation call since SUM,MIN and MAX are asssociative (with a slight
   * disregard to floating-point arithmetic). There are specializations needed for StandardDeviationSampling
   * and CountDistinct due to their non-standard nature of accumulators.
  */
  for (auto step_index = uint64_t{0}; step_index < new_group_count; ++step_index) {
    const auto [source_index, other_index] = merge_plan[step_index];
    if (other_index == -1 && source_index > -1) {
      merged->accumulators[step_index] = std::move(source_results->accumulators[source_index]);
      merged->counts[step_index] = source_results->counts[source_index];
    } else if (source_index == -1 && other_index > -1) {
      merged->accumulators[step_index] = std::move(other_results->accumulators[other_index]);
      merged->counts[step_index] = other_results->counts[other_index];
    } else {
      /*
        * We use an extension of Welford's online algorith, however not adjusted for the case where both counts are large
        * and roughly the same (see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm).
      */
      if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
        auto& accumulator_a = source_results->accumulators[source_index];
        auto& accumulator_b = other_results->accumulators[other_index];

        const auto count_a = accumulator_a[0];
        const auto count_b = accumulator_b[0];
        const auto new_count = count_a + count_b;
        const auto delta_mean = accumulator_b[1] - accumulator_a[1];
        const auto updated_mean = accumulator_a[1] + (delta_mean * (count_b / new_count));
        const auto updated_squared_distance =
            accumulator_a[2] + accumulator_b[2] +
            ((delta_mean * delta_mean) * (static_cast<double>(count_a * count_b) / new_count));

        accumulator_a[0] = new_count;
        accumulator_a[1] = updated_mean;
        accumulator_a[2] = updated_squared_distance;
        if (new_count > 1) {
          // The SQL standard defines VAR_SAMP (which is the basis of STDDEV_SAMP) as NULL if the number of values is 1.
          const auto variance = updated_squared_distance / (new_count - 1);
          accumulator_a[3] = std::sqrt(variance);
        }
      } else if constexpr (aggregate_function == WindowFunction::CountDistinct) {
        // For CountDistinct we merge the sorted sets of distinct values with a linear merge.
        pmr_vector<AggregateType> new_values;
        const auto& source_values = source_results->accumulators[source_index];
        const auto& other_values = other_results->accumulators[other_index];
        new_values.reserve(source_values.size() + other_values.size());
        std::ranges::set_union(source_values, other_values, std::back_inserter(new_values));
        // We set these values so that after the update of the count and accumulator that happens
        // in every case the count is the number of unique values and the accumulator contains
        // the new set.
        source_results->counts[source_index] = new_values.size();
        other_results->counts[other_index] = 0;
        source_results->accumulators[source_index] = std::move(new_values);
      } else if (other_results->counts[other_index] != 0) {
        aggregator(other_results->accumulators[other_index], source_results->counts[source_index],
                   source_results->accumulators[source_index]);
      }
      merged->counts[step_index] = source_results->counts[source_index] + other_results->counts[other_index];
      merged->accumulators[step_index] = std::move(source_results->accumulators[source_index]);
    }
  }

  aggregate_results[aggregate_index] = std::move(merged);
}

template <WindowFunction aggregate_function, typename AggregateType>
std::shared_ptr<ValueSegment<AggregateType>> AggregateDYOD::Morsel::to_value_segment(uint64_t aggregate_index,
                                                                                      bool nullable) {
  /*
   * To convert the accumulated values to output values we have to distinguish the aggregate_functions
   * as in some cases the values cannot be read straight out of the accumulator, e.g. for AVG, we have to divide the
   * accumulated value (which is just the sum) by the number of values. We then write these values into vectors
   * with which we create ValueSegments that can be added to the results table.
  */
  using Results = AggregateResults<aggregate_function, AggregateType>;
  const auto value_count = group_count;
  auto values = pmr_vector<AggregateType>(value_count);
  auto null_values = pmr_vector<bool>(value_count);
  const auto results = std::static_pointer_cast<Results>(aggregate_results[aggregate_index]);
  const auto& accumulators = results->accumulators;
  const auto& counts = results->counts;

  for (auto group_index = uint64_t{0}; group_index < group_count; ++group_index) {
    const auto count = counts[group_index];
    const auto& accumulator = accumulators[group_index];

    if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
      if (accumulator[0] > 1) {
        values[group_index] = accumulator[3];
      } else {
        null_values[group_index] = true;
      }
    } else if constexpr (aggregate_function == WindowFunction::Count) {
      values[group_index] = static_cast<int64_t>(count);
    } else if constexpr (aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
      if (count == 0) {
        null_values[group_index] = true;
      } else {
        values[group_index] = accumulator / static_cast<AggregateType>(count);
      }
    } else {
      if (count == 0) {
        null_values[group_index] = true;
      } else {
        values[group_index] = accumulator;
      }
    }
  }

  if (nullable) {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }
  return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
}

template <typename ColumnType>
std::shared_ptr<ValueSegment<int64_t>> AggregateDYOD::Morsel::distinct_to_value_segment(
    const uint64_t aggregate_index) {
  using Results = AggregateResults<WindowFunction::CountDistinct, ColumnType>;
  const auto results = std::static_pointer_cast<Results>(aggregate_results[aggregate_index]);
  const auto& counts = results->counts;

  auto values = pmr_vector<int64_t>(group_count);

  for (auto group_index = uint64_t{0}; group_index < group_count; ++group_index) {
    values[group_index] = static_cast<int64_t>(counts[group_index]);
  }

  return std::make_shared<ValueSegment<int64_t>>(std::move(values));
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {
  _aggregate_column_position.clear();
  _unique_aggregate_columns.clear();
  _materialized_aggregate_columns.clear();
}

template <typename ColumnType, typename AggregateType, WindowFunction aggregate_function>
std::shared_ptr<ValueSegment<AggregateType>> AggregateDYOD::_aggregate_values_without_groups(
    const uint64_t aggregate_index) {
  /*
   * For the Aggregation without groups we do not need to perform checks for groups. We also
   * perform the transformation into the proper values and creation of the ValueSegments
   * right away (unlike with to_value_segment()) since we already have all the data in one place 
   * after the aggregation is finished.
  */
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*_aggregates[aggregate_index]->argument());
  const auto input_column_id = pqp_column.column_id;
  auto value_count = uint64_t{0};

  auto accumulated_values = pmr_vector<AggregateType>();
  auto accumulated_null_values = pmr_vector<bool>();

  auto accumulator = AggregateAccumulator<aggregate_function, AggregateType>{};
  auto is_null = (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);
  auto aggregator = WindowFunctionBuilder<ColumnType, AggregateType, aggregate_function>().get_aggregate_function();
  auto distinct_values = std::unordered_set<ColumnType>();

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  if constexpr (aggregate_function == WindowFunction::Count) {
    accumulated_values.push_back(input_table->row_count());
    return std::make_shared<ValueSegment<AggregateType>>(std::move(accumulated_values));
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnType>(*segment, [&](const auto& position) {
      const auto& new_value = position.value();
      if (!position.is_null()) {
        is_null = false;
        aggregator(new_value, value_count, accumulator);
        ++value_count;
        if constexpr (aggregate_function == WindowFunction::CountDistinct) {
          distinct_values.insert(new_value);
        }
      }
    });
  }

  if constexpr (aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    if (value_count > 0) {
      accumulator = accumulator / static_cast<AggregateType>(value_count);
    }
  }

  if constexpr (aggregate_function == WindowFunction::CountDistinct && std::is_arithmetic_v<AggregateType>) {
    accumulator = size(distinct_values);
  }

  if constexpr (aggregate_function == WindowFunction::StandardDeviationSample && std::is_arithmetic_v<ColumnType>) {
    if (value_count >= 2) {
      accumulated_values.push_back(accumulator[3]);
    } else {
      // STDDEV_SAMP is undefined for lists with less than two elements
      is_null = true;
    }
  }

  if constexpr (aggregate_function != WindowFunction::StandardDeviationSample) {
    accumulated_values.push_back(accumulator);
  }

  accumulated_null_values.push_back(is_null);

  if constexpr (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct) {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(accumulated_values),
                                                         std::move(accumulated_null_values));
  } else {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(accumulated_values));
  }
}

template <typename ColumnType>
void AggregateDYOD::create_aggregate_column_definitions(boost::hana::basic_type<ColumnType> /*type*/,
                                                         ColumnID column_index, WindowFunction aggregate_function) {
  /*
   * We are aware that the switch looks very repetitive, but we could not find a dynamic solution.
   * There is a similar switch statement in _on_execute for calling _aggregate_values.
   * See the comment there for reasoning.
   */
    resolve_window_function(aggregate_function, [&](auto window_function_constant) {
      constexpr auto AGGREGATE_FUNCTION = decltype(window_function_constant)::value;

      create_aggregate_column_definitions<ColumnType, AGGREGATE_FUNCTION>(column_index);
    });
}

template <typename ColumnType, WindowFunction aggregate_function>
void AggregateDYOD::create_aggregate_column_definitions(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  auto result_type = WindowFunctionTraits<ColumnType, aggregate_function>::RESULT_TYPE;

  const auto& aggregate = _aggregates[column_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (result_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    result_type = left_input_table()->column_data_type(input_column_id);
  }

  // Count (Distinct) columns are never nullable as there is always an available count and an ANY column
  // can only be nullable as long as the column it is referring to was also nullable. All other aggregations
  // resolve the first conjunction to true and are thus always nullable.
  const auto nullable =
      (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct &&
       aggregate_function != WindowFunction::Any) ||
      (aggregate_function == WindowFunction::Any && left_input_table()->column_is_nullable(input_column_id));
  const auto column_name =
      aggregate->window_function == WindowFunction::Any ? pqp_column.as_column_name() : aggregate->as_column_name();
  _output_column_definitions.emplace_back(column_name, result_type, nullable);
}

}  // namespace hyrise
