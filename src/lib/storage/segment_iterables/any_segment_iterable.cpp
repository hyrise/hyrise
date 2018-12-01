#include "any_segment_iterable.hpp"

#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"

namespace opossum {

template <typename T>
AnySegmentIterable<T> create_any_segment_iterable(const BaseSegment& base_segment) {
  auto any_segment_iterable = std::optional<AnySegmentIterable<T>>{};

  resolve_segment_type<T>(base_segment, [&](const auto& segment) {
    const auto actual_iterable = create_iterable_from_segment<T>(segment);
    any_segment_iterable.emplace(erase_type_from_iterable(actual_iterable));
  });

  return std::move(*any_segment_iterable);
}

template AnySegmentIterable<int32_t> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<int64_t> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<float> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<double> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<std::string> create_any_segment_iterable(const BaseSegment& base_segment);

}  // namespace opossum
