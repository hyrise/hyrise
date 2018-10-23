#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/reference_segment.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/**
 * A SegmentAccessor is templated per SegmentType and DataType (T).
 * It requires that the underlying segment implements an implicit interface:
 *
 *   const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;
 *
 */
template <typename T, typename SegmentType>
class SegmentAccessor : public BaseSegmentAccessor<T> {
 public:
  explicit SegmentAccessor(const SegmentType& segment) : _segment{segment} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _segment.get_typed_value(offset); }

 protected:
  const SegmentType& _segment;
};

/**
 * Partial template specialization for ReferenceSegments.
 * Since ReferenceSegments don't know their 'T', this uses the subscript operator as a fallback.
 *
 * Under normal circumstances, using a SegmentAccessor on a ReferenceSegment does not make sense, as
 * it is not faster (potentially even slower) than using operator[] directly on the ReferenceSegment.
 * However, this spezialization is still useful in the case that the underlying segment type
 * is not directly known to the calling code and extra code paths for ReferenceSegments should be avoided.
 */
template <typename T>
class SegmentAccessor<T, ReferenceSegment> : public BaseSegmentAccessor<T> {
 public:
  explicit SegmentAccessor(const ReferenceSegment& segment) : _segment{segment} {}
  const std::optional<T> access(ChunkOffset offset) const final {
    PerformanceWarning("SegmentAccessor used on ReferenceSegment");
    const auto all_type_variant = _segment[offset];
    if (variant_is_null(all_type_variant)) {
      return std::nullopt;
    }
    return type_cast<T>(all_type_variant);
  }

 protected:
  const ReferenceSegment& _segment;
};

/**
 * Utility method to create a SegmentAccessor for a given BaseSegment.
 */
template <typename T>
std::unique_ptr<BaseSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment) {
  std::unique_ptr<BaseSegmentAccessor<T>> accessor;
  resolve_segment_type<T>(*segment, [&](const auto& typed_segment) {
    using SegmentType = std::decay_t<decltype(typed_segment)>;
    accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
  });
  return accessor;
}

}  // namespace opossum
