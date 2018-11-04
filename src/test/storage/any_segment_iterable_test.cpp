#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

class AnySegmentIterableTest : public BaseTest {
 protected:
  static constexpr auto row_count = 200u;

  void SetUp() override { _segment = create_int_w_null_value_segment(); }

  std::shared_ptr<ValueSegment<int32_t>> create_int_w_null_value_segment() {
    auto values = pmr_concurrent_vector<int32_t>(row_count);
    auto null_values = pmr_concurrent_vector<bool>(row_count);

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, 10u};
    std::bernoulli_distribution bernoulli_dist{0.3};

    for (auto i = 0u; i < row_count; ++i) {
      values[i] = dist(engine);
      null_values[i] = bernoulli_dist(engine);
    }

    return std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<PosList> create_sequential_position_filter() {
    auto list = std::make_shared<PosList>();
    list->guarantee_single_chunk();

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < row_count; ++offset_in_referenced_chunk) {
      if (bernoulli_dist(engine)) {
        list->push_back(RowID{ChunkID{0}, offset_in_referenced_chunk});
      }
    }

    return list;
  }

 protected:
  std::shared_ptr<ValueSegment<int32_t>> _segment;
};

TEST_F(AnySegmentIterableTest, SequentiallyIterateOverSegment) {
  auto iterable = ValueSegmentIterable<int32_t>{*_segment};
  auto any_iterable = AnySegmentIterable{iterable};

  iterable.with_iterators([&](auto it, auto end) {
    any_iterable.with_iterators([&](auto any_it, auto any_end) {
      for (; any_it != any_end; ++any_it, ++it) {
        EXPECT_EQ(it->is_null(), any_it->is_null());

        if (!it->is_null()) {
          EXPECT_EQ(it->value(), any_it->value());
        }
      }
    });
  });
}

TEST_F(AnySegmentIterableTest, RandomlyIterateOverSegment) {
  auto iterable = ValueSegmentIterable<int32_t>{*_segment};
  auto any_iterable = AnySegmentIterable{iterable};

  const auto chunk_offsets_list = create_sequential_position_filter();

  iterable.with_iterators(chunk_offsets_list, [&](auto it, auto end) {
    any_iterable.with_iterators(chunk_offsets_list, [&](auto any_it, auto any_end) {
      for (; any_it != any_end; ++any_it, ++it) {
        EXPECT_EQ(it->is_null(), any_it->is_null());

        if (!it->is_null()) {
          EXPECT_EQ(it->value(), any_it->value());
        }
      }
    });
  });
}

}  // namespace opossum
