#pragma once

#include <memory>
#include <cstddef>

#include "all_type_variant.hpp"
#include "abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
class DummySegment : public AbstractSegment {
 public:
  explicit DummySegment(DataType data_type, ChunkOffset alleged_size);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;
  ChunkOffset size() const override;
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;
  size_t memory_usage(const MemoryUsageCalculationMode mode) const override;

 protected:
  DataType _own_data_type;
  ChunkOffset _alleged_size;
};
} // namespace hyrise
