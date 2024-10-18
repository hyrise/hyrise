#pragma once

#include <vector>
#include <cstddef>

#include "storage/segment_iterables.hpp"
#include "storage/dummy_segment.hpp"

namespace hyrise {
template <typename T>
class DummySegmentIterable : public SegmentIterable<DummySegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit DummySegmentIterable(const DummySegment<T>& segment) : _segment(segment) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    functor(_dummy_vector.begin(), _dummy_vector.end());
  }

  size_t _on_size() const {
    return 0; // TODO(JEH) _segment.size()?
  }

 protected:
  const DummySegment<T>& _segment;
  const std::vector<T> _dummy_vector;
};
} // namespace hyrise
