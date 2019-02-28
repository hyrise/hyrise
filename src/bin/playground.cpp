#include <iostream>

#include "types.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

using namespace opossum;  // NOLINT

int main() {
  pmr_concurrent_vector<int32_t> values;
  values.push_back(1);
  values.push_back(5);
  values.push_back(6);
  values.push_back(19);

  const auto segment = std::make_shared<ValueSegment<int32_t>>(std::move(values));

  const auto iterable = ValueSegmentIterable<int32_t>(*segment);

  iterable.with_iterators([&](const auto begin, const auto end) {
    auto iter = std::upper_bound(begin, end, 8, [&](const auto& value, const auto& segment_position) {
      return value < segment_position.value();
    });

    if (iter == end) {
      std::cout << "Not found" << std::endl;
    } else {
      std::cout << "Lower Bound: " << iter->value() << std::endl;
    }
  });

  return 0;
}
