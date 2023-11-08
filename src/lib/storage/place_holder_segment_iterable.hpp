#pragma once

#include <type_traits>
#include <vector>

#include "storage/segment_iterables.hpp"
#include "storage/place_holder_segment.hpp"

namespace hyrise {

template <typename T>
class PlaceHolderSegmentIterable : public PointAccessibleSegmentIterable<PlaceHolderSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit PlaceHolderSegmentIterable(const PlaceHolderSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    // std::cout << "Sequential filter iterable" << std::endl;

    auto v = std::vector<SegmentPosition<T>>{};
    functor(v.begin(), v.end());
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    const auto position_filter_size = position_filter->size();
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter_size;

    // std::cout << "Positional filter iterable" << std::endl;

    auto v = std::vector<SegmentPosition<T>>{};
    functor(v.begin(), v.end());
  }

  size_t _on_size() const {
    return _segment.size();
  }

 private:
  const PlaceHolderSegment& _segment;
};

}  // namespace hyrise
