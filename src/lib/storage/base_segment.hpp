#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// BaseSegment is the abstract super class for all segment types,
// e.g., ValueSegment, ReferenceSegment
class BaseSegment : private Noncopyable {
 public:
  explicit BaseSegment(const DataType data_type);
  virtual ~BaseSegment() = default;

  // the type of the data contained in this segment
  DataType data_type() const;

  // returns the value at a given position
  virtual AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // returns the number of values
  virtual ChunkOffset size() const = 0;

  // Copies a segment using a new allocator. This is useful for placing the segment on a new NUMA node.
  virtual std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const = 0;

  // Estimate how much memory the segment is using.
  // Might be inaccurate, especially if the segment contains non-primitive data,
  // such as strings who memory usage is implementation defined
  virtual size_t memory_usage(const MemoryUsageCalculationMode mode) const = 0;

 private:
  const DataType _data_type;
};
}  // namespace opossum
