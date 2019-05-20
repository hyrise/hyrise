#include "base_test.hpp"
#include "operators/aggregate/aggregate_hashsort_steps.hpp"

using namespace opossum::aggregate_hashsort;  // NOLINT

namespace opossum {

class AggregateHashSortStepsTest : public ::testing::Test {
 public:
  void SetUp() override {

  }

};

TEST_F(AggregateHashSortStepsTest, Partitioning) {
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(1), 1);
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(2), 0);
}

TEST_F(AggregateHashSortStepsTest, Partition) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {0}};
  auto groups = FixedSizeGroupRun{groups_layout, 7};

  // clang-format off
  groups.hashes[0] = size_t{0b001}; groups.data[0] = int32_t{5};
  groups.hashes[1] = size_t{0b100}; groups.data[1] = int32_t{2};
  groups.hashes[2] = size_t{0b101}; groups.data[2] = int32_t{5};
  groups.hashes[3] = size_t{0b110}; groups.data[3] = int32_t{6};
  groups.hashes[4] = size_t{0b000}; groups.data[4] = int32_t{7};
  groups.hashes[5] = size_t{0b100}; groups.data[5] = int32_t{8};
  groups.hashes[6] = size_t{0b100}; groups.data[6] = int32_t{8};
  // clang-format off

  auto run = opossum::aggregate_hashsort::Run<FixedSizeGroupRun>{std::move(groups), {}};
  auto runs = std::vector<opossum::aggregate_hashsort::Run<FixedSizeGroupRun>>{};
  runs.emplace_back(std::move(run));

  auto partitioning = Partitioning{4, 1, 3};
  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  AggregateHashSortConfig config;
  config.max_partitioning_counter = 6;

  const auto partitions = partition(config, std::move(runs), partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 0);
  EXPECT_EQ(run_offset, 6);

  ASSERT_EQ(partitions.size(), 4u);

  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 0u);
  ASSERT_EQ(partitions.at(2).runs.size(), 1u);
  ASSERT_EQ(partitions.at(3).runs.size(), 1u);

  ASSERT_EQ(partitions.at(0).runs.at(0).groups.data, std::vector<uint32_t>({5, 7}));
  ASSERT_EQ(partitions.at(0).runs.at(0).groups.hashes, std::vector<size_t>({0b001, 0b000}));

  ASSERT_EQ(partitions.at(2).runs.at(0).groups.data, std::vector<uint32_t>({2, 5, 8}));
  ASSERT_EQ(partitions.at(2).runs.at(0).groups.hashes, std::vector<size_t>({0b100, 0b101, 0b100}));

  ASSERT_EQ(partitions.at(3).runs.at(0).groups.data, std::vector<uint32_t>({6}));
  ASSERT_EQ(partitions.at(3).runs.at(0).groups.hashes, std::vector<size_t>({0b110}));
}

TEST_F(AggregateHashSortStepsTest, Hashing) {
  auto groups_layout = FixedSizeGroupRunLayout{2, {0, 4}};
  auto groups = FixedSizeGroupRun{groups_layout, 4};

  // clang-format off
  groups.hashes[0] = size_t{12}; groups.data[0] = int32_t{5}; groups.data[1] = int32_t{3};
  groups.hashes[1] = size_t{13}; groups.data[2] = int32_t{2}; groups.data[3] = int32_t{2};
  groups.hashes[2] = size_t{12}; groups.data[4] = int32_t{5}; groups.data[5] = int32_t{3};
  groups.hashes[3] = size_t{12}; groups.data[6] = int32_t{5}; groups.data[7] = int32_t{4};
  // clang-format on

  auto run = opossum::aggregate_hashsort::Run<FixedSizeGroupRun>{std::move(groups), {}};
  auto runs = std::vector<opossum::aggregate_hashsort::Run<FixedSizeGroupRun>>{};
  runs.emplace_back(std::move(run));

  auto partitioning = Partitioning{2, 0, 1};
  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  // Config hash table so that the entire input is hashed
  AggregateHashSortConfig config;
  config.hash_table_size = 100;
  config.hash_table_max_load_factor = 1.0f;

  const auto [continue_hashing, partitions] = hashing(config, std::move(runs), partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 1);
  EXPECT_EQ(run_offset, 0);
  EXPECT_TRUE(continue_hashing);

  ASSERT_EQ(partitions.size(), 2u);

  ASSERT_EQ(partitions.at(0).size(), 2u);
  ASSERT_EQ(partitions.at(1).size(), 1u);

  ASSERT_EQ(partitions.at(0).runs.at(0).groups.data, std::vector<uint32_t>({5, 3, 5, 4}));
  ASSERT_EQ(partitions.at(0).runs.at(0).groups.hashes, std::vector<size_t>({12, 12}));

  ASSERT_EQ(partitions.at(1).runs.at(0).groups.data, std::vector<uint32_t>({2, 2}));
  ASSERT_EQ(partitions.at(1).runs.at(0).groups.hashes, std::vector<size_t>({13}));
}

}  // namespace opossum
