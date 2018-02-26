#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/column_iterables/any_column_iterable.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

class AnyColumnIterableTest : public BaseTest {
 protected:
  static constexpr auto row_count = 200u;

  void SetUp() override { _column = create_int_w_null_value_column(); }

  std::shared_ptr<ValueColumn<int32_t>> create_int_w_null_value_column() {
    auto values = pmr_concurrent_vector<int32_t>(row_count);
    auto null_values = pmr_concurrent_vector<bool>(row_count);

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, 10u};
    std::bernoulli_distribution bernoulli_dist{0.3};

    for (auto i = 0u; i < row_count; ++i) {
      values[i] = dist(engine);
      null_values[i] = bernoulli_dist(engine);
    }

    return std::make_shared<ValueColumn<int32_t>>(std::move(values), std::move(null_values));
  }

  ChunkOffsetsList create_sequential_chunk_offsets_list() {
    auto list = ChunkOffsetsList{};

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto into_referencing = 0u, into_referenced = 0u; into_referenced < row_count; ++into_referenced) {
      if (bernoulli_dist(engine)) {
        list.push_back({into_referencing++, into_referenced});
      }
    }

    return list;
  }

 protected:
  std::shared_ptr<ValueColumn<int32_t>> _column;
};

TEST_F(AnyColumnIterableTest, SequentiallyIterateOverColumn) {
  auto iterable = ValueColumnIterable<int32_t>{*_column};
  auto any_iterable = AnyColumnIterable{iterable};

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

TEST_F(AnyColumnIterableTest, RandomlyIterateOverColumn) {
  auto iterable = ValueColumnIterable<int32_t>{*_column};
  auto any_iterable = AnyColumnIterable{iterable};

  const auto chunk_offsets_list = create_sequential_chunk_offsets_list();

  iterable.with_iterators(&chunk_offsets_list, [&](auto it, auto end) {
    any_iterable.with_iterators(&chunk_offsets_list, [&](auto any_it, auto any_end) {
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
