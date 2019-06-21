#include "segment_accessor.hpp"

#include "resolve_type.hpp"

namespace opossum {

namespace detail {
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> CreateSegmentAccessor<T>::create(
    const std::shared_ptr<const BaseSegment>& segment) {
  std::unique_ptr<AbstractSegmentAccessor<T>> accessor;
  resolve_segment_type<T>(*segment, [&](const auto& typed_segment) {
    using SegmentType = std::decay_t<decltype(typed_segment)>;
    if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
      const auto& pos_list = *typed_segment.pos_list();
      if (pos_list.references_single_chunk() && pos_list.size() > 0) {
        // If the first chunk_id references a non-existing chunk, the entry is NULL. Since all entries reference
        // the same chunk_id, we can safely assume that all other entries are also NULL and always return std::nullopt.
        if (pos_list[ChunkOffset{0}].is_null()) {
          accessor = std::make_unique<NullAccessor<T>>();
        } else {
          auto chunk_id = pos_list[ChunkOffset{0}].chunk_id;
          auto referenced_segment =
              typed_segment.referenced_table()->get_chunk(chunk_id)->get_segment(typed_segment.referenced_column_id());

          // If only a single segment is referenced, we can resolve it right here and now so that we can avoid some
          // virtual method calls later.
          resolve_segment_type<T>(*referenced_segment, [&](const auto& typed_referenced_segment) {
            using ReferencedSegment = std::decay_t<decltype(typed_referenced_segment)>;
            if constexpr (!std::is_same_v<ReferencedSegment, ReferenceSegment>) {
              accessor = std::make_unique<SingleChunkReferenceSegmentAccessor<T, ReferencedSegment>>(
                  pos_list, chunk_id, typed_referenced_segment);
            } else {
              Fail("Encountered nested ReferenceSegments");
            }
          });
        }
      } else {
        accessor = std::make_unique<MultipleChunkReferenceSegmentAccessor<T>>(typed_segment);
      }
    } else {
      accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
    }
  });
  return accessor;
}
EXPLICITLY_INSTANTIATE_DATA_TYPES(CreateSegmentAccessor);
}  // namespace detail

}  // namespace opossum
