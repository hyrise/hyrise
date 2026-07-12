#include "operators/aggregate_dyod/accumulator_column.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

// The methods below are Fail() stubs so the value and accumulator columns' tests compile and link; the actual
// implementations are added test-driven in subsequent steps. Fail()-only bodies trip -Wmissing-noreturn; the real
// implementations will return.
#pragma clang diagnostic ignored "-Wmissing-noreturn"

namespace hyrise {

template <typename T>
NumericValueScatterColumn<T>::NumericValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {
  static_cast<void>(_source_column);
  static_cast<void>(_nullable);
}

template <typename T>
uint32_t NumericValueScatterColumn<T>::element_width() const {
  Fail("NumericValueScatterColumn::element_width is not implemented yet.");
}

template <typename T>
bool NumericValueScatterColumn<T>::is_nullable() const {
  Fail("NumericValueScatterColumn::is_nullable is not implemented yet.");
}

template <typename T>
void NumericValueScatterColumn<T>::pack(const AbstractSegment& /*segment*/, const ChunkOffset /*chunk_offset*/,
                                        std::byte* /*value_dest*/, std::byte* /*null_bitmap*/,
                                        const uint32_t /*null_bit_index*/, StringSpillBuffer& /*value_arena*/) const {
  Fail("NumericValueScatterColumn::pack is not implemented yet.");
}

template class NumericValueScatterColumn<int32_t>;
template class NumericValueScatterColumn<int64_t>;
template class NumericValueScatterColumn<float>;
template class NumericValueScatterColumn<double>;

StringValueScatterColumn::StringValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {
  static_cast<void>(_source_column);
  static_cast<void>(_nullable);
}

uint32_t StringValueScatterColumn::element_width() const {
  Fail("StringValueScatterColumn::element_width is not implemented yet.");
}

bool StringValueScatterColumn::is_nullable() const {
  Fail("StringValueScatterColumn::is_nullable is not implemented yet.");
}

void StringValueScatterColumn::pack(const AbstractSegment& /*segment*/, const ChunkOffset /*chunk_offset*/,
                                    std::byte* /*value_dest*/, std::byte* /*null_bitmap*/,
                                    const uint32_t /*null_bit_index*/, StringSpillBuffer& /*value_arena*/) const {
  Fail("StringValueScatterColumn::pack is not implemented yet.");
}

template <typename ColumnType, WindowFunction Function>
TypedAccumulatorColumn<ColumnType, Function>::TypedAccumulatorColumn() = default;

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::grow_to(const size_t /*slot_count*/) {
  Fail("TypedAccumulatorColumn::grow_to is not implemented yet.");
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::fold(std::span<const uint32_t> /*slots*/,
                                                        std::span<const std::byte> /*value_bytes*/,
                                                        std::span<const std::byte> /*value_null_bitmap*/) {
  Fail("TypedAccumulatorColumn::fold is not implemented yet.");
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::clear() {
  Fail("TypedAccumulatorColumn::clear is not implemented yet.");
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::finalize_into(const size_t /*first_slot*/,
                                                                 const size_t /*last_slot*/,
                                                                 const size_t /*output_column_index*/,
                                                                 OutputColumns& /*output*/) const {
  Fail("TypedAccumulatorColumn::finalize_into is not implemented yet.");
}

AggregateSchema AggregateSchema::build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& /*aggregates*/,
                                       const Table& /*input_table*/) {
  Fail("AggregateSchema::build is not implemented yet.");
}

size_t AggregateSchema::aggregate_count() const {
  Fail("AggregateSchema::aggregate_count is not implemented yet.");
}

DataType AggregateSchema::result_type(const size_t /*aggregate_index*/) const {
  Fail("AggregateSchema::result_type is not implemented yet.");
}

size_t AggregateSchema::value_stream_count() const {
  Fail("AggregateSchema::value_stream_count is not implemented yet.");
}

const AbstractValueScatterColumn& AggregateSchema::value_stream(const size_t /*stream_index*/) const {
  Fail("AggregateSchema::value_stream is not implemented yet.");
}

size_t AggregateSchema::aggregate_value_stream(const size_t /*aggregate_index*/) const {
  Fail("AggregateSchema::aggregate_value_stream is not implemented yet.");
}

size_t AggregateSchema::value_null_bitmap_width() const {
  static_cast<void>(_value_null_bitmap_width);
  Fail("AggregateSchema::value_null_bitmap_width is not implemented yet.");
}

bool AggregateSchema::needs_value_arena() const {
  Fail("AggregateSchema::needs_value_arena is not implemented yet.");
}

std::vector<std::unique_ptr<AbstractAccumulatorColumn>> AggregateSchema::make_accumulator_columns() const {
  Fail("AggregateSchema::make_accumulator_columns is not implemented yet.");
}

}  // namespace hyrise
