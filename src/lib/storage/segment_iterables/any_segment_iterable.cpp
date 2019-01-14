#include "any_segment_iterable.hpp" // NEEDEDINCLUDE

#include "storage/create_iterable_from_segment.hpp" // NEEDEDINCLUDE
#include "storage/segment_iterables/any_segment_iterable.hpp" // NEEDEDINCLUDE

namespace opossum {

namespace detail {
template <typename T>
AnySegmentIterable<T> CreateAnySegmentIterable<T>::create(const BaseSegment& base_segment) {
  auto any_segment_iterable = std::optional<AnySegmentIterable<T>>{};

  resolve_segment_type<T>(base_segment, [&](const auto& segment) {
    const auto actual_iterable = create_iterable_from_segment<T>(segment);
    any_segment_iterable.emplace(erase_type_from_iterable(actual_iterable));
  });

  return std::move(*any_segment_iterable);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CreateAnySegmentIterable);
}  // namespace detail

}  // namespace opossum
