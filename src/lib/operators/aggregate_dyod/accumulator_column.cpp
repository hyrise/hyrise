#include "operators/aggregate_dyod/accumulator_column.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;

// Fixed-width cell a string value stream writes: a pointer into the per-partition value arena plus the length.
struct StringValueReference {
  const std::byte* data;
  uint64_t length;
};

void set_null_bit(std::byte* null_bitmap, const uint32_t bit_index) {
  null_bitmap[bit_index / 8] |= std::byte{1} << (bit_index % 8);
}

bool null_bit_set(const std::byte* null_bitmap, const size_t bit_index) {
  return (null_bitmap[bit_index / 8] & (std::byte{1} << (bit_index % 8))) != std::byte{0};
}

DataType resolve_result_type(const DataType input_type, const WindowFunction function) {
  auto result_type = DataType::Null;
  resolve_data_type(input_type, [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (function) {
      case WindowFunction::Min:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::RESULT_TYPE;
        break;
      case WindowFunction::Max:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::RESULT_TYPE;
        break;
      case WindowFunction::Sum:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::RESULT_TYPE;
        break;
      case WindowFunction::Avg:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::RESULT_TYPE;
        break;
      case WindowFunction::Count:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::RESULT_TYPE;
        break;
      default:
        Fail("Unsupported aggregate function.");
    }
  });
  return result_type;
}

}  // namespace

namespace hyrise {

template <typename T>
NumericValueScatterColumn<T>::NumericValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {}

template <typename T>
uint32_t NumericValueScatterColumn<T>::element_width() const {
  return sizeof(T);
}

template <typename T>
bool NumericValueScatterColumn<T>::is_nullable() const {
  return _nullable;
}

template <typename T>
void NumericValueScatterColumn<T>::pack(const AbstractSegment& segment, const ChunkOffset chunk_offset,
                                        std::byte* value_dest, std::byte* null_bitmap, const uint32_t null_bit_index,
                                        StringSpillBuffer& /*value_arena*/) const {
  const auto variant = segment[chunk_offset];
  if (variant_is_null(variant)) {
    DebugAssert(_nullable, "NULL in a non-nullable value column.");
    set_null_bit(null_bitmap, null_bit_index);
    return;
  }
  const auto value = boost::get<T>(variant);
  std::memcpy(value_dest, &value, sizeof(value));
}

template class NumericValueScatterColumn<int32_t>;
template class NumericValueScatterColumn<int64_t>;
template class NumericValueScatterColumn<float>;
template class NumericValueScatterColumn<double>;

StringValueScatterColumn::StringValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {}

uint32_t StringValueScatterColumn::element_width() const {
  return sizeof(StringValueReference);
}

bool StringValueScatterColumn::is_nullable() const {
  return _nullable;
}

void StringValueScatterColumn::pack(const AbstractSegment& segment, const ChunkOffset chunk_offset,
                                    std::byte* value_dest, std::byte* null_bitmap, const uint32_t null_bit_index,
                                    StringSpillBuffer& value_arena) const {
  const auto variant = segment[chunk_offset];
  if (variant_is_null(variant)) {
    DebugAssert(_nullable, "NULL in a non-nullable value column.");
    set_null_bit(null_bitmap, null_bit_index);
    return;
  }
  const auto value = boost::get<pmr_string>(variant);
  auto reference = StringValueReference{};
  reference.data = value_arena.append(reinterpret_cast<const std::byte*>(value.data()), value.size());
  reference.length = value.size();
  std::memcpy(value_dest, &reference, sizeof(reference));
}

template <typename ColumnType, WindowFunction Function>
TypedAccumulatorColumn<ColumnType, Function>::TypedAccumulatorColumn() = default;

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::grow_to(const size_t slot_count) {
  DebugAssert(slot_count >= _accumulators.size(), "Dense accumulator state only grows.");
  _accumulators.resize(slot_count);
  if constexpr (Function != WindowFunction::Count) {
    _non_null_counts.resize(slot_count, 0);
  }
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::fold(std::span<const uint32_t> slots,
                                                        std::span<const std::byte> value_bytes,
                                                        std::span<const std::byte> value_null_bitmap) {
  const auto row_count = slots.size();
  DebugAssert(value_null_bitmap.empty() || value_null_bitmap.size() * 8 >= row_count,
              "Value-null bitmap does not cover the tile.");
  if constexpr (Function != WindowFunction::Count) {
    constexpr auto VALUE_WIDTH =
        std::is_same_v<ColumnType, pmr_string> ? sizeof(StringValueReference) : sizeof(ColumnType);
    DebugAssert(value_bytes.size() == row_count * VALUE_WIDTH, "Value tile does not match the slot tile.");
  }

  if constexpr (Function == WindowFunction::Count) {
    if (value_bytes.empty()) {
      for (auto row = size_t{0}; row < row_count; ++row) {
        ++_accumulators[slots[row]];
      }
      return;
    }
  }

  for (auto row = size_t{0}; row < row_count; ++row) {
    if (!value_null_bitmap.empty() && null_bit_set(value_null_bitmap.data(), row)) {
      continue;
    }
    const auto slot = slots[row];
    if constexpr (Function == WindowFunction::Count) {
      ++_accumulators[slot];
    } else if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      auto reference = StringValueReference{};
      std::memcpy(&reference, value_bytes.data() + row * sizeof(reference), sizeof(reference));
      const auto value = std::string_view{reinterpret_cast<const char*>(reference.data), reference.length};
      auto& count = _non_null_counts[slot];
      auto& current = _accumulators[slot];
      if constexpr (Function == WindowFunction::Min) {
        if (count == 0 || value < current) {
          current = pmr_string{value};
        }
      } else if constexpr (Function == WindowFunction::Max) {
        if (count == 0 || value > current) {
          current = pmr_string{value};
        }
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    } else {
      auto value = ColumnType{};
      std::memcpy(&value, value_bytes.data() + row * sizeof(value), sizeof(value));
      auto& count = _non_null_counts[slot];
      if constexpr (Function == WindowFunction::Min) {
        if (count == 0 || value < _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (Function == WindowFunction::Max) {
        if (count == 0 || value > _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (Function == WindowFunction::Sum || Function == WindowFunction::Avg) {
        _accumulators[slot] += static_cast<AccumulatorType>(value);
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    }
  }
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::clear() {
  _accumulators.clear();
  _non_null_counts.clear();
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::finalize_into(const size_t first_slot, const size_t last_slot,
                                                                 const size_t output_column_index,
                                                                 OutputColumns& output) const {
  constexpr auto IS_MIN_MAX_OR_SUM =
      Function == WindowFunction::Min || Function == WindowFunction::Max || Function == WindowFunction::Sum;
  auto& output_column = static_cast<TypedOutputColumn<AccumulatorType>&>(output.column(output_column_index));
  for (auto slot = first_slot; slot < last_slot; ++slot) {
    if constexpr (Function == WindowFunction::Count) {
      output_column.append(_accumulators[slot]);
    } else if constexpr (Function == WindowFunction::Avg) {
      if (_non_null_counts[slot] == 0) {
        output_column.append_null();
      } else {
        output_column.append(_accumulators[slot] / static_cast<AccumulatorType>(_non_null_counts[slot]));
      }
    } else if constexpr (IS_MIN_MAX_OR_SUM) {
      if (_non_null_counts[slot] == 0) {
        output_column.append_null();
      } else {
        output_column.append(_accumulators[slot]);
      }
    } else {
      Fail("Unsupported aggregate function.");
    }
  }
}

AggregateSchema AggregateSchema::build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                                       const Table& input_table) {
  auto schema = AggregateSchema{};
  auto nullable_stream_count = size_t{0};

  for (const auto& aggregate : aggregates) {
    const auto function = aggregate->window_function;
    const auto pqp_column = std::dynamic_pointer_cast<PQPColumnExpression>(aggregate->argument());
    Assert(pqp_column, "Aggregates must reference a column.");
    const auto source_column = pqp_column->column_id;

    auto entry = AggregateEntry{};
    entry.source_column = source_column;
    entry.function = function;

    if (source_column == INVALID_COLUMN_ID) {
      Assert(function == WindowFunction::Count, "Only COUNT(*) may aggregate without a source column.");
      entry.input_type = DataType::Null;
      entry.result_type = DataType::Long;
      entry.value_stream_index = NO_VALUE_STREAM;
      schema._entries.emplace_back(entry);
      continue;
    }

    entry.input_type = input_table.column_data_type(source_column);
    entry.result_type = resolve_result_type(entry.input_type, function);
    Assert(entry.result_type != DataType::Null, "Invalid aggregate function for the source column's data type.");

    entry.value_stream_index = NO_VALUE_STREAM;
    for (const auto& earlier : schema._entries) {
      if (earlier.source_column == source_column) {
        entry.value_stream_index = earlier.value_stream_index;
        break;
      }
    }
    if (entry.value_stream_index == NO_VALUE_STREAM) {
      const auto nullable = input_table.column_is_nullable(source_column);
      nullable_stream_count += nullable ? 1 : 0;
      entry.value_stream_index = schema._value_streams.size();
      resolve_data_type(entry.input_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          schema._value_streams.emplace_back(std::make_unique<StringValueScatterColumn>(source_column, nullable));
        } else {
          schema._value_streams.emplace_back(
              std::make_unique<NumericValueScatterColumn<ColumnDataType>>(source_column, nullable));
        }
      });
    }
    schema._entries.emplace_back(entry);
  }

  schema._value_null_bitmap_width = static_cast<uint32_t>((nullable_stream_count + 7) / 8);
  return schema;
}

size_t AggregateSchema::aggregate_count() const {
  return _entries.size();
}

DataType AggregateSchema::result_type(const size_t aggregate_index) const {
  return _entries[aggregate_index].result_type;
}

size_t AggregateSchema::value_stream_count() const {
  return _value_streams.size();
}

const AbstractValueScatterColumn& AggregateSchema::value_stream(const size_t stream_index) const {
  return *_value_streams[stream_index];
}

size_t AggregateSchema::aggregate_value_stream(const size_t aggregate_index) const {
  return _entries[aggregate_index].value_stream_index;
}

size_t AggregateSchema::value_null_bitmap_width() const {
  return _value_null_bitmap_width;
}

bool AggregateSchema::needs_value_arena() const {
  for (const auto& entry : _entries) {
    if (entry.value_stream_index != NO_VALUE_STREAM && entry.input_type == DataType::String) {
      return true;
    }
  }
  return false;
}

std::vector<std::unique_ptr<AbstractAccumulatorColumn>> AggregateSchema::make_accumulator_columns() const {
  auto columns = std::vector<std::unique_ptr<AbstractAccumulatorColumn>>{};
  columns.reserve(_entries.size());

  for (const auto& entry : _entries) {
    if (entry.value_stream_index == NO_VALUE_STREAM) {
      columns.emplace_back(std::make_unique<TypedAccumulatorColumn<int64_t, WindowFunction::Count>>());
      continue;
    }
    resolve_data_type(entry.input_type, [&](const auto type) {
      using ColumnDataType = typename decltype(type)::type;
      switch (entry.function) {
        case WindowFunction::Min:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Min>>());
          break;
        case WindowFunction::Max:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Max>>());
          break;
        case WindowFunction::Count:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Count>>());
          break;
        case WindowFunction::Sum:
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Sum>>());
            break;
          }
          Fail("SUM requires an arithmetic source column.");
        case WindowFunction::Avg:
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Avg>>());
            break;
          }
          Fail("AVG requires an arithmetic source column.");
        default:
          Fail("Unsupported aggregate function.");
      }
    });
  }
  return columns;
}

}  // namespace hyrise
