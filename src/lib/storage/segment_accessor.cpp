#include "segment_accessor.hpp"

namespace opossum {

/**
 * Utility method to create a SegmentAccessor for a given BaseSegment.
 */
template <typename T>
std::unique_ptr<BaseSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment) {
  std::unique_ptr<BaseSegmentAccessor<T>> accessor;
  resolve_segment_type<T>(*segment, [&](const auto& typed_segment) {
    using SegmentType = std::decay_t<decltype(typed_segment)>;
    if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
      if (typed_segment.pos_list()->references_single_chunk() && typed_segment.pos_list()->size() > 0) {
        accessor = std::make_unique<SingleChunkReferenceSegmentAccessor<T>>(typed_segment);
      } else {
        accessor = std::make_unique<MultipleChunkReferenceSegmentAccessor<T>>(typed_segment);
      }
    } else {
      accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
    }
  });
  return accessor;
}

template std::unique_ptr<BaseSegmentAccessor<int32_t>> create_segment_accessor(
    const std::shared_ptr<const BaseSegment>& segment);
template std::unique_ptr<BaseSegmentAccessor<int64_t>> create_segment_accessor(
    const std::shared_ptr<const BaseSegment>& segment);
template std::unique_ptr<BaseSegmentAccessor<float>> create_segment_accessor(
    const std::shared_ptr<const BaseSegment>& segment);
template std::unique_ptr<BaseSegmentAccessor<double>> create_segment_accessor(
    const std::shared_ptr<const BaseSegment>& segment);
template std::unique_ptr<BaseSegmentAccessor<std::string>> create_segment_accessor(
    const std::shared_ptr<const BaseSegment>& segment);

}  // namespace opossum
