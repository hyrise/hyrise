#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "segment_access_counter.hpp"
#include "types.hpp"

namespace hyrise {

// AbstractSegment is the abstract super class for all segment types, e.g., ValueSegment or ReferenceSegment.
class AbstractSegment : private Noncopyable {
 public:
  explicit AbstractSegment(const DataType data_type);

  virtual ~AbstractSegment() = default;

  // The type of the data contained in this segment.
  DataType data_type() const;

  // Returns the value at a given position.
  virtual AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // Returns the number of values.
  virtual ChunkOffset size() const = 0;

  // Copies a segment using a new allocator. This is useful for placing the segment on a new NUMA node.
  virtual std::shared_ptr<AbstractSegment> copy_using_memory_resource(MemoryResource& memory_resource) const = 0;

  // Estimate how much memory the segment is using. Might be inaccurate, especially if the segment contains
  // non-primitive data, such as strings, whose memory usage is implementation-defined.
  virtual size_t memory_usage(const MemoryUsageCalculationMode mode) const = 0;

  mutable SegmentAccessCounter access_counter;

 private:
  const DataType _data_type;
};
}  // namespace hyrise
