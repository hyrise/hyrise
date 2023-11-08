#include "place_holder_segment.hpp"

#include <memory>

#include "abstract_segment.hpp"
#include "chunk.hpp"

namespace hyrise {

PlaceHolderSegment::PlaceHolderSegment(const std::shared_ptr<Table>& base_table, const ColumnID column_id, bool nullable, ChunkOffset capacity)
    : AbstractSegment(base_table->column_data_type(column_id)),
     _base_table{base_table}, _column_id{column_id}, _nullable{nullable}, _capacity{capacity} {
    // We need to have a way to call the plugin later.
  }

AllTypeVariant PlaceHolderSegment::operator[](const ChunkOffset chunk_offset) const {
  Fail("operator[] cannot be called on place holder segments. Use iterables.");
}

ChunkOffset PlaceHolderSegment::size() const {
  return Chunk::DEFAULT_SIZE;
}

std::shared_ptr<AbstractSegment> PlaceHolderSegment::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  Fail("PlaceHolder segments cannot be copied.");
}

size_t PlaceHolderSegment::memory_usage(const MemoryUsageCalculationMode mode) const {
  Fail("The size of PlaceHolder segments should not be requested as data is not yet loaded.");
}

}  // namespace hyrise
