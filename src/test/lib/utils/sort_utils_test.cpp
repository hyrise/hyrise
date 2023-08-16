#include <random>
#include <ranges>  // NOLINT(build/include_order)
#include <vector>

#include "base_test.hpp"

#include "utils/sort_utils.hpp"

namespace hyrise {

class ParallelInplaceMergeSortTest : public BaseTest {};

TEST_F(ParallelInplaceMergeSortTest, Million) {
  auto gen = std::mt19937_64(42);
  auto dist = std::uniform_int_distribution<>(-42, 20'000);

  auto data = std::vector<int>(1'000'000);

  for (auto& element : data) {
    element = dist(gen);
  }

  const auto comparator3way = [](const auto& lhs, const auto& rhs) { return (lhs + 5) <=> (rhs + 5); };
  const auto comparator = [&comparator3way](const auto& lhs, const auto& rhs) {
    return std::is_lt(comparator3way(lhs, rhs));
  };

  auto std_sorted_data = data;
  std::ranges::sort(std_sorted_data, comparator);

  parallel_inplace_merge_sort<int, 8, 256>(data, comparator3way);
  EXPECT_EQ(data, std_sorted_data);
}

}  // namespace hyrise
