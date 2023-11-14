#pragma once

#include <type_traits>
#include <vector>

#include "hyrise.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/place_holder_segment.hpp"
#include "utils/data_loading_utils.hpp"

namespace hyrise {

template <typename T>
class PlaceHolderSegmentIterable : public PointAccessibleSegmentIterable<PlaceHolderSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit PlaceHolderSegmentIterable(const PlaceHolderSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    const auto& loaded_segment = _segment.load_and_return_segment();
    auto iterable = create_any_segment_iterable<T>(*loaded_segment);
    iterable.with_iterators([&](auto it, const auto end) {
      functor(it, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    const auto position_filter_size = position_filter->size();
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter_size;

    const auto& loaded_segment = _segment.load_and_return_segment();
    auto iterable = create_any_segment_iterable<T>(*loaded_segment);
    iterable.with_iterators(position_filter, [&](auto it, const auto end) {
      functor(it, end);
    });
  }

  size_t _on_size() const {
    return _segment.size();
  }

 private:
  const PlaceHolderSegment& _segment;
};

}  // namespace hyrise
