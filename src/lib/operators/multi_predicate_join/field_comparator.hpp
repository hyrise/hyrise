#pragma once

#include "base_field_comparator.hpp"
#include "storage/base_segment_accessor.hpp"

using namespace opossum;

namespace mpj {

template<typename CompareFunctor, typename L, typename R>
class FieldComparator : public BaseFieldComparator {
 public:
  FieldComparator(CompareFunctor compare_functor,
                  std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> left_accessors,
                  std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> right_accessors)
      : _compare{std::move(compare_functor)},
        _left_accessors{std::move(left_accessors)},
        _right_accessors{std::move(right_accessors)} {}

  bool compare(const RowID& left, const RowID& right) const override {
    const auto left_value = _left_accessors[left.chunk_id]->access(left.chunk_offset);
    const auto right_value = _right_accessors[right.chunk_id]->access(right.chunk_offset);

    // NULL value handling:
    // If either left or right value is NULL, the comparison will evaluate to false.
    if (!left_value || !right_value) {
      return false;
    } else {
      return _compare(*left_value, *right_value);
    }
  }

 private:
  const CompareFunctor _compare;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>>
      _left_accessors;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>>
      _right_accessors;
};

} // namespace mpj