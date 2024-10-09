#pragma once

#include "abstract_segment.hpp"

namespace hyrise {
// TODO: cleanup constness and header imports everywhere

class DummySegment : public AbstractSegment {
 public:
  explicit DummySegment(DataType data_type, ChunkOffset alleged_size);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const;
  ChunkOffset size() const;
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;
  size_t memory_usage(const MemoryUsageCalculationMode mode) const;

 protected:
  DataType _own_data_type;
  ChunkOffset _alleged_size;
};
} // namespace hyrise
