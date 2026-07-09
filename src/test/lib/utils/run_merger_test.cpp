#include <functional>
#include <vector>

#include "base_test.hpp"
#include "testing_assert.hpp"
#include "utils/run_merger.hpp"

namespace hyrise {

class RunMergerTest : public BaseTest {};

TEST_F(RunMergerTest, Sorted) {
  constexpr auto RUN_SIZE = 16;
  constexpr auto DATA_SIZE = RUN_SIZE * RUN_SIZE;
  auto input_buffer = std::vector<uint64_t>(DATA_SIZE);
  auto output_buffer = std::vector<uint64_t>(DATA_SIZE);
  auto sorted_runs = std::vector<std::span<uint64_t>>();

  // Fill input buffer with sorted data.
  std::ranges::iota(input_buffer, 0);
  EXPECT_TRUE(std::ranges::is_sorted(input_buffer));

  // Create sorted runs as partitions of sorted input.
  for (auto offset = size_t{0}; offset < DATA_SIZE; offset += RUN_SIZE) {
    const auto it = input_buffer.begin() + offset;
    sorted_runs.emplace_back(it, RUN_SIZE);
  }
  EXPECT_EQ(sorted_runs.size(), DATA_SIZE / RUN_SIZE);

  RunMerger<uint64_t>::merge(sorted_runs, output_buffer);

  EXPECT_TRUE(std::ranges::is_sorted(output_buffer));
  for (auto idx = size_t{0}; idx < DATA_SIZE; ++idx) {
    EXPECT_EQ(input_buffer[idx], output_buffer[idx]);
  }
}

}  // namespace hyrise
