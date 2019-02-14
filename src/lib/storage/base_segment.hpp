#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "chunk_encoder.hpp"
#include "pos_list.hpp"
#include "types.hpp"
#include "utils/format_bytes.hpp"

namespace opossum {

class AbstractSegmentVisitor;
class SegmentVisitorContext;

// BaseSegment is the abstract super class for all segment types,
// e.g., ValueSegment, ReferenceSegment
class BaseSegment : private Noncopyable {
 public:
  explicit BaseSegment(const DataType data_type);
  virtual ~BaseSegment() = default;

  // the type of the data contained in this segment
  DataType data_type() const;

  // returns the value at a given position
  virtual const AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // Copies a segment using a new allocator. This is useful for placing the segment on a new NUMA node.
  virtual std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const = 0;

  // Estimate how much memory the segment is using.
  // Might be inaccurate, especially if the segment contains non-primitive data,
  // such as strings who memory usage is implementation defined
  virtual size_t estimate_memory_usage() const = 0;

  std::optional<OrderByMode> sort_order() const;
  void set_sort_order(OrderByMode sort_order);

  // TODO(cmfcmf): These methods should be marked with "= 0" once all segments implement them.

  virtual ChunkOffset get_non_null_begin_offset(const std::shared_ptr<const PosList>& position_filter = nullptr) const
      /*= 0*/; // NOLINT
  virtual ChunkOffset get_non_null_end_offset(const std::shared_ptr<const PosList>& position_filter = nullptr) const
      /*= 0 */; // NOLINT

  virtual ChunkOffset get_first_offset(const AllTypeVariant& search_value,
                                       const std::shared_ptr<const PosList>& position_filter = nullptr) const /*= 0*/;
  virtual ChunkOffset get_last_offset(const AllTypeVariant& search_value,
                                      const std::shared_ptr<const PosList>& position_filter = nullptr) const /*= 0*/;

 protected:
  std::optional<OrderByMode> _sort_order;

 private:
  const DataType _data_type;
};
}  // namespace opossum
