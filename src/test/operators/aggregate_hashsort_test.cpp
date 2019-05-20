#include "base_test.hpp"
#include "operators/aggregate/aggregate_hashsort_steps.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum::aggregate_hashsort;  // NOLINT

namespace {

//struct IdentityHash {
//  template<typename T>
//  size_t operator()(const T& value) const {
//    if constexpr (!std::is_same_v<T, opossum::pmr_string>) {
//      return static_cast<size_t>(value);
//    } else {
//      return value.size();
//    }
//  }
//};

template <typename S>
uint32_t lower_word(const S& s) {
  static_assert(sizeof(S) == 8);
  uint32_t t;
  memcpy(&t, &s, 4);
  return t;
}

template <typename S>
uint32_t upper_word(const S& s) {
  static_assert(sizeof(S) == 8);
  uint32_t t;
  memcpy(&t, &reinterpret_cast<const char*>(&s)[4], 4);
  return t;
}

template <typename T, typename S>
T bit_cast(const S& s) {
  static_assert(sizeof(T) == sizeof(S));
  T t;
  memcpy(&t, &s, sizeof(T));
  return t;
}

}  // namespace

namespace opossum {

class AggregateHashSortStepsTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(AggregateHashSortStepsTest, ProduceInitialGroupsFixed) {
  auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});

  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{1}, ColumnID{0}, ColumnID{2}};

  auto groups = produce_initial_groups<FixedSizeGroupRun>(*table, group_by_column_ids);

  EXPECT_EQ(groups.layout.group_size, 6u);
  EXPECT_EQ(groups.layout.column_base_offsets, std::vector<size_t>({0, 1, 3}));

  EXPECT_EQ(groups.hashes.size(), 3u);

  // clang-format off
  const auto expected_group_data = std::vector<uint32_t>{
    13, 0, bit_cast<uint32_t>(4.5f), 0, lower_word(1.0), upper_word(1.0),
    12, 1, 0, 0, lower_word(-2.0), upper_word(-2.0),
    14, 0, bit_cast<uint32_t>(1.5f), 1, 0, 0
  };
  // clang-format on

  EXPECT_EQ(groups.data, expected_group_data);
}

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

TEST_F(AggregateHashSortStepsTest, AggregateAdaptive) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {0}};
  auto groups = FixedSizeGroupRun{groups_layout, 8};

  // clang-format off
  groups.hashes[0] = size_t{0b1}; groups.data[0] = int32_t{5};
  groups.hashes[1] = size_t{0b1}; groups.data[1] = int32_t{3};
  groups.hashes[2] = size_t{0b1}; groups.data[2] = int32_t{5};
  groups.hashes[3] = size_t{0b0}; groups.data[3] = int32_t{2};
  groups.hashes[4] = size_t{0b1}; groups.data[4] = int32_t{3};
  groups.hashes[5] = size_t{0b1}; groups.data[5] = int32_t{5};
  groups.hashes[6] = size_t{0b0}; groups.data[6] = int32_t{2};
  groups.hashes[7] = size_t{0b0}; groups.data[7] = int32_t{2};
  // clang-format off

  // Config so that [0,3] are hashed, [4,5] are partitioned and [6,7] are hashed again
  AggregateHashSortConfig config;
  config.hash_table_size = 4;
  config.hash_table_max_load_factor = 0.5f;
  config.max_partitioning_counter = 2;

  auto run = opossum::aggregate_hashsort::Run<FixedSizeGroupRun>{std::move(groups), {}};
  auto runs = std::vector<opossum::aggregate_hashsort::Run<FixedSizeGroupRun>>{};
  runs.emplace_back(std::move(run));

  auto partitioning = Partitioning{2, 0, 1};

  const auto partitions = adaptive_hashing_and_partition(config, std::move(runs), partitioning);

  ASSERT_EQ(partitions.size(), 2u);

  ASSERT_EQ(partitions.at(0).size(), 2u);
  ASSERT_EQ(partitions.at(1).size(), 4u);

  ASSERT_EQ(partitions.at(0).runs.size(), 2u);
  ASSERT_EQ(partitions.at(1).runs.size(), 2u);

  EXPECT_TRUE(partitions.at(0).runs.at(0).is_aggregated);
  EXPECT_TRUE(partitions.at(0).runs.at(1).is_aggregated);
  EXPECT_TRUE(partitions.at(1).runs.at(0).is_aggregated);
  EXPECT_FALSE(partitions.at(1).runs.at(1).is_aggregated);

  EXPECT_EQ(partitions.at(0).runs.at(0).groups.data, std::vector<uint32_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.hashes, std::vector<size_t>({0b0}));
  EXPECT_EQ(partitions.at(0).runs.at(1).groups.data, std::vector<uint32_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(1).groups.hashes, std::vector<size_t>({0b0}));

  EXPECT_EQ(partitions.at(1).runs.at(0).groups.data, std::vector<uint32_t>({5, 3}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.hashes, std::vector<size_t>({0b1, 0b1}));
  EXPECT_EQ(partitions.at(1).runs.at(1).groups.data, std::vector<uint32_t>({3, 5}));
  EXPECT_EQ(partitions.at(1).runs.at(1).groups.hashes, std::vector<size_t>({0b1, 0b1}));
}

}  // namespace opossum
