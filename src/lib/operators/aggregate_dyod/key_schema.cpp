#include "operators/aggregate_dyod/key_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

// The methods below are Fail() stubs so the key schema's tests compile and link; the actual implementations are added
// test-driven in subsequent steps. Fail()-only bodies trip -Wmissing-noreturn; the real implementations will return.
#ifdef __clang__
#pragma clang diagnostic ignored "-Wmissing-noreturn"
#endif

namespace hyrise {

const std::byte* StringSpillBuffer::append(const std::byte* /*content*/, size_t /*length*/) {
  Fail("StringSpillBuffer::append is not implemented yet.");
}

void StringSpillBuffer::clear() {
  Fail("StringSpillBuffer::clear is not implemented yet.");
}

template <size_t PackedWidth>
NumericShortKeySchema<PackedWidth> NumericShortKeySchema<PackedWidth>::build(
    const std::vector<ColumnID>& /*group_by_column_ids*/, const Table& /*input_table*/) {
  Fail("NumericShortKeySchema::build is not implemented yet.");
}

template <size_t PackedWidth>
size_t NumericShortKeySchema<PackedWidth>::packed_width() const {
  Fail("NumericShortKeySchema::packed_width is not implemented yet.");
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::pack(std::span<const AbstractSegment* const> /*group_by_segments*/,
                                              ChunkOffset /*chunk_offset*/, std::byte* /*key_out*/,
                                              StringSpillBuffer& /*spill_buffer*/) const {
  Fail("NumericShortKeySchema::pack is not implemented yet.");
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::unpack(const std::byte* /*key*/, OutputColumns& /*output*/,
                                                size_t /*output_row*/) const {
  Fail("NumericShortKeySchema::unpack is not implemented yet.");
}

template <size_t PackedWidth>
uint64_t NumericShortKeySchema<PackedWidth>::hash(const std::byte* /*key*/) const {
  Fail("NumericShortKeySchema::hash is not implemented yet.");
}

template <size_t PackedWidth>
bool NumericShortKeySchema<PackedWidth>::equals(const std::byte* /*a*/, const std::byte* /*b*/) const {
  Fail("NumericShortKeySchema::equals is not implemented yet.");
}

template class NumericShortKeySchema<4>;
template class NumericShortKeySchema<8>;
template class NumericShortKeySchema<12>;
template class NumericShortKeySchema<16>;

NumericArbitraryKeySchema NumericArbitraryKeySchema::build(const std::vector<ColumnID>& /*group_by_column_ids*/,
                                                           const Table& /*input_table*/) {
  Fail("NumericArbitraryKeySchema::build is not implemented yet.");
}

size_t NumericArbitraryKeySchema::packed_width() const {
  static_cast<void>(_packed_width);
  Fail("NumericArbitraryKeySchema::packed_width is not implemented yet.");
}

void NumericArbitraryKeySchema::pack(std::span<const AbstractSegment* const> /*group_by_segments*/,
                                     ChunkOffset /*chunk_offset*/, std::byte* /*key_out*/,
                                     StringSpillBuffer& /*spill_buffer*/) const {
  Fail("NumericArbitraryKeySchema::pack is not implemented yet.");
}

void NumericArbitraryKeySchema::unpack(const std::byte* /*key*/, OutputColumns& /*output*/,
                                       size_t /*output_row*/) const {
  Fail("NumericArbitraryKeySchema::unpack is not implemented yet.");
}

uint64_t NumericArbitraryKeySchema::hash(const std::byte* /*key*/) const {
  Fail("NumericArbitraryKeySchema::hash is not implemented yet.");
}

bool NumericArbitraryKeySchema::equals(const std::byte* /*a*/, const std::byte* /*b*/) const {
  Fail("NumericArbitraryKeySchema::equals is not implemented yet.");
}

}  // namespace hyrise
