#include "segment_accessor.hpp"

namespace opossum {

namespace detail {
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> CreateSegmentAccessor<T>::create(
    const std::shared_ptr<const BaseSegment>& segment) {
  std::unique_ptr<AbstractSegmentAccessor<T>> accessor;
  resolve_segment_type<T>(*segment, [&](const auto& typed_segment) {
      using SegmentType = std::decay_t<decltype(typed_segment)>;
      if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
          if (typed_segment.pos_list()->references_single_chunk() && typed_segment.pos_list()->size() > 0) {
              accessor = std::make_unique<SingleChunkReferenceSegmentAccessor<T>>(typed_segment);
          } else {
              accessor = std::make_unique<MultipleChunkReferenceSegmentAccessor<T>>(typed_segment);
          }
      } else if constexpr (std::is_same_v<SegmentType, ValueSegment<T>>) {
          accessor = std::make_unique<ValueSegmentAccessor<T>>(typed_segment);
      } else if constexpr (std::is_same_v<SegmentType, DictionarySegment<T>>) {
          accessor = std::make_unique<DictionarySegmentAccessor<T>>(typed_segment);
      } else {
          accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
      }
  });
  return accessor;
}
EXPLICITLY_INSTANTIATE_DATA_TYPES(CreateSegmentAccessor);
}  // namespace detail

}  // namespace opossum