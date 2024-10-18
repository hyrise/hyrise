#pragma once

#include <memory>
#include <cstddef>

#include "all_type_variant.hpp"
#include "abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
template <typename T>
class DummySegment : public AbstractSegment {
 public:
  explicit DummySegment(ChunkOffset alleged_size);

  bool supports_reencoding() const override;
  bool has_actual_data() const override;

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;
  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;
  ChunkOffset size() const override;
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;
  size_t memory_usage(const MemoryUsageCalculationMode mode) const override;

 protected:
  ChunkOffset _alleged_size;
};

EXPLICITLY_DECLARE_DATA_TYPES(DummySegment);
} // namespace hyrise
