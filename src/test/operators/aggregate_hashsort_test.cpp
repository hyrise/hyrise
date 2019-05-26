#include "base_test.hpp"
#include "operators/abstract_aggregate_operator.hpp"
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

VariablySizedGroupRun::BlobDataType chars_to_blob_element(const std::string& chars) {
  Assert(chars.size() <= sizeof(VariablySizedGroupRun::BlobDataType), "Invalid string size");

  auto blob_element = VariablySizedGroupRun::BlobDataType{};
  memcpy(&blob_element, chars.data(), chars.size());
  return blob_element;
}

}  // namespace

namespace opossum {

class AggregateHashSortStepsTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(AggregateHashSortStepsTest, ProduceInitialGroupsFixed) {
  const auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});

  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{1}, ColumnID{0}, ColumnID{2}};

  const auto layout = produce_initial_groups_layout<FixedSizeGroupRunLayout>(*table, group_by_column_ids);
  auto groups = produce_initial_groups<FixedSizeGroupRun>(*table, &layout, group_by_column_ids);

  EXPECT_EQ(groups.layout->group_size, 6u);
  EXPECT_EQ(groups.layout->column_base_offsets, std::vector<size_t>({0, 1, 3}));

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

TEST_F(AggregateHashSortStepsTest, ProduceInitialGroupsFixedEmpty) {
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Float, true}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f});
  table->append({2.5f});

  const auto group_by_column_ids = std::vector<ColumnID>{};

  const auto layout = produce_initial_groups_layout<FixedSizeGroupRunLayout>(*table, group_by_column_ids);
  auto groups = produce_initial_groups<FixedSizeGroupRun>(*table, &layout, group_by_column_ids);

  EXPECT_EQ(groups.layout->group_size, 0u);
  EXPECT_EQ(groups.layout->column_base_offsets, std::vector<size_t>());

  EXPECT_EQ(groups.hashes.size(), 2u);

  EXPECT_EQ(groups.data, std::vector<uint32_t>());
}

TEST_F(AggregateHashSortStepsTest, ProduceInitialGroupsLayoutVariablySized) {
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                         {"b", DataType::String, false},
                                                         {"c", DataType::String, true},
                                                         {"d", DataType::Int, false},
                                                         {"e", DataType::Long, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{4}};

  const auto layout = produce_initial_groups_layout<VariablySizedGroupRunLayout>(*table, group_by_column_ids);

  ASSERT_EQ(layout.variably_sized_column_ids, std::vector<ColumnID>({ColumnID{1}, ColumnID{2}}));
  EXPECT_EQ(layout.fixed_size_column_ids, std::vector<ColumnID>({ColumnID{0}, ColumnID{4}}));
  EXPECT_EQ(layout.column_count, 2);
  EXPECT_EQ(layout.nullable_column_count, 1);
  ASSERT_EQ(layout.column_mapping.size(), 4u);
  EXPECT_EQ(layout.column_mapping.at(0), VariablySizedGroupRunLayout::Column(false, size_t{0}, std::nullopt));
  EXPECT_EQ(layout.column_mapping.at(1), VariablySizedGroupRunLayout::Column(true, size_t{0}, std::nullopt));
  EXPECT_EQ(layout.column_mapping.at(2), VariablySizedGroupRunLayout::Column(true, size_t{1}, size_t{0}));
  EXPECT_EQ(layout.column_mapping.at(3), VariablySizedGroupRunLayout::Column(false, size_t{1}, std::nullopt));
  EXPECT_EQ(layout.fixed_layout.group_size, 4);
  EXPECT_EQ(layout.fixed_layout.column_base_offsets, std::vector<size_t>({0, 2}));
}

TEST_F(AggregateHashSortStepsTest, ProduceInitialGroupsVariablySized) {
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                         {"b", DataType::String, false},
                                                         {"c", DataType::String, true},
                                                         {"d", DataType::Int, false},
                                                         {"e", DataType::Long, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({11, "abcd", "bcdef", 0, int64_t{12}});
  table->append({NullValue{}, "aa", "bcd", 1, int64_t{14}});
  table->append({NullValue{}, "aa", "bcd", 1, int64_t{14}});
  table->append({NullValue{}, "aa", NullValue{}, 1, int64_t{14}});

  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{4}};

  const auto layout = produce_initial_groups_layout<VariablySizedGroupRunLayout>(*table, group_by_column_ids);
  auto groups = produce_initial_groups<VariablySizedGroupRun>(*table, &layout, group_by_column_ids);

  // clang-format off
  const auto expected_variably_sized_group_data = std::vector<VariablySizedGroupRun::BlobDataType>{
    chars_to_blob_element("abcd"), chars_to_blob_element("bcde"), chars_to_blob_element("f"),
    chars_to_blob_element("aabc"), chars_to_blob_element("d"),
    chars_to_blob_element("aabc"), chars_to_blob_element("d"),
    chars_to_blob_element("aa"),
  };
  // clang-format on

  EXPECT_EQ(groups.data, expected_variably_sized_group_data);
  EXPECT_EQ(groups.null_values, std::vector<bool>({false, false, false, true}));
  EXPECT_EQ(groups.group_end_offsets, std::vector<size_t>({3, 5, 7, 8}));
  EXPECT_EQ(groups.value_end_offsets, std::vector<size_t>({4, 9, 2, 5, 2, 5, 2, 2}));
  EXPECT_EQ(groups.fixed.hashes.size(), 4u);

  // clang-format off
  const auto expected_fixed_group_data = std::vector<uint32_t>{
    0, 11, lower_word(int64_t{12}), upper_word(int64_t{12}),
    1, 0, lower_word(int64_t{14}), upper_word(int64_t{14}),
    1, 0, lower_word(int64_t{14}), upper_word(int64_t{14}),
    1, 0, lower_word(int64_t{14}), upper_word(int64_t{14})
  };
  // clang-format on

  EXPECT_EQ(groups.fixed.data, expected_fixed_group_data);
}

TEST_F(AggregateHashSortStepsTest, ProduceInitialAggregates) {
  auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});

  auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{
      {std::nullopt, AggregateFunction::CountRows},
      {ColumnID{0}, AggregateFunction::Sum},
      {ColumnID{1}, AggregateFunction::Min},
      {ColumnID{2}, AggregateFunction::Avg},
  };

  auto aggregates = produce_initial_aggregates(*table, aggregate_column_definitions, false);

  ASSERT_EQ(aggregates.size(), 4u);

  const auto* count_rows = dynamic_cast<CountRowsAggregateRun*>(aggregates[0].get());
  ASSERT_TRUE(count_rows);
  EXPECT_EQ(count_rows->values, std::vector<int64_t>({1, 1, 1}));

  const auto* sum = dynamic_cast<SumAggregateRun<float>*>(aggregates[1].get());
  ASSERT_TRUE(sum);
  EXPECT_EQ(sum->values, std::vector<double>({4.5, 0.0f, 1.5}));
  EXPECT_EQ(sum->null_values, std::vector({false, true, false}));

  const auto* min = dynamic_cast<MinAggregateRun<int32_t>*>(aggregates[2].get());
  ASSERT_TRUE(min);
  EXPECT_EQ(min->values, std::vector<int32_t>({13, 12, 14}));
  EXPECT_EQ(min->null_values, std::vector({false, false, false}));

  const auto* avg = dynamic_cast<AvgAggregateRun<double>*>(aggregates[3].get());
  ASSERT_TRUE(avg);
  EXPECT_EQ(avg->sets, std::vector<std::vector<double>>({{1.0}, {-2.0}, {}}));
}
TEST_F(AggregateHashSortStepsTest, Partitioning) {
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(1), 1);
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(2), 0);
}

TEST_F(AggregateHashSortStepsTest, PartitionFixedOnly) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {0}};
  auto groups = FixedSizeGroupRun{&groups_layout};
  groups.resize(7);

  // clang-format off
  groups.hashes[0] = size_t{0b001}; groups.data[0] = int32_t{5};
  groups.hashes[1] = size_t{0b100}; groups.data[1] = int32_t{2};
  groups.hashes[2] = size_t{0b101}; groups.data[2] = int32_t{5};
  groups.hashes[3] = size_t{0b110}; groups.data[3] = int32_t{6};
  groups.hashes[4] = size_t{0b000}; groups.data[4] = int32_t{7};
  groups.hashes[5] = size_t{0b100}; groups.data[5] = int32_t{8};
  groups.hashes[6] = size_t{0b100}; groups.data[6] = int32_t{8};
  // clang-format off

  auto aggregate = std::make_unique<SumAggregateRun<int32_t>>();
  // clang-format off
  aggregate->values =      {0,     1,     2,     3,     0,    5,     6,     7};
  aggregate->null_values = {false, false, false, false, true, false, false, false};
  // clang-format on
  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>();
  aggregates.emplace_back(std::move(aggregate));

  auto run = opossum::aggregate_hashsort::Run<FixedSizeGroupRun>{std::move(groups), std::move(aggregates)};
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
  ASSERT_EQ(partitions.at(0).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_0 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(0).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_0);
  EXPECT_EQ(aggregate_0->values, std::vector<int64_t>({0, 0}));
  EXPECT_EQ(aggregate_0->null_values, std::vector<bool>({false, true}));

  ASSERT_EQ(partitions.at(2).runs.at(0).groups.data, std::vector<uint32_t>({2, 5, 8}));
  ASSERT_EQ(partitions.at(2).runs.at(0).groups.hashes, std::vector<size_t>({0b100, 0b101, 0b100}));
  ASSERT_EQ(partitions.at(2).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_2 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(2).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_2);
  EXPECT_EQ(aggregate_2->values, std::vector<int64_t>({1, 2, 5}));
  EXPECT_EQ(aggregate_2->null_values, std::vector<bool>({false, false, false}));

  ASSERT_EQ(partitions.at(3).runs.at(0).groups.data, std::vector<uint32_t>({6}));
  ASSERT_EQ(partitions.at(3).runs.at(0).groups.hashes, std::vector<size_t>({0b110}));
  ASSERT_EQ(partitions.at(3).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_3 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(3).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_3);
  EXPECT_EQ(aggregate_3->values, std::vector<int64_t>({3}));
  EXPECT_EQ(aggregate_3->null_values, std::vector<bool>({false}));
}

TEST_F(AggregateHashSortStepsTest, PartitionVariablySizedAndFixed) {
  // clang-format off
  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>{
    {true, ColumnID{0}, std::nullopt},
    {false, ColumnID{0}, std::nullopt},
    {true, ColumnID{1}, 0},
    {false, ColumnID{1}, std::nullopt},
    {true, ColumnID{2}, 1},
  };
  // clang-format on

  auto fixed_layout = FixedSizeGroupRunLayout{0, {}};
  const auto layout = VariablySizedGroupRunLayout{{ColumnID{0}, ColumnID{1}, ColumnID{2}}, {}, column_mapping, fixed_layout};
  auto groups = VariablySizedGroupRun{&layout};

  // clang-format off
  groups.data = {
    chars_to_blob_element("hell"), chars_to_blob_element("owor"), chars_to_blob_element("ldwh"), chars_to_blob_element("y"), // NOLINT
    chars_to_blob_element("yesn"), chars_to_blob_element("omay"), chars_to_blob_element("be"),
    chars_to_blob_element("yet"),
    chars_to_blob_element("grea"), chars_to_blob_element("tbad"), chars_to_blob_element("go"),
  };
  groups.group_end_offsets = {4, 7, 8, 11};
  groups.value_end_offsets = {
    5, 10, 14,
    4, 6, 11,
    4, 4, 4,
    6, 9, 13
  };
  groups.null_values = {
    false, false,
    false, false,
    true, true,
    false, false
  };
  groups.fixed.hashes = {0b1, 0b0, 0b0, 0b1};
  // clang-format on

  auto partitioning = Partitioning{2, 0, 1};
  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>();

  auto run = opossum::aggregate_hashsort::Run<VariablySizedGroupRun>{std::move(groups), std::move(aggregates)};
  auto runs = std::vector<opossum::aggregate_hashsort::Run<VariablySizedGroupRun>>{};
  runs.emplace_back(std::move(run));

  AggregateHashSortConfig config;
  config.max_partitioning_counter = 100; // Partition the entire run

  const auto partitions = partition(config, std::move(runs), partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 1);
  EXPECT_EQ(run_offset, 0);

  ASSERT_EQ(partitions.size(), 2);

  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  // clang-format off
  const auto partition_0_expected_data = std::vector<VariablySizedGroupRun::BlobDataType >{
    chars_to_blob_element("yesn"), chars_to_blob_element("omay"), chars_to_blob_element("be"),
    chars_to_blob_element("yet")
  };
  // clang-format on
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.data, partition_0_expected_data);
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.group_end_offsets, std::vector<size_t>({3, 4}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.value_end_offsets, std::vector<size_t>({4, 6, 11, 4, 4, 4}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.null_values, std::vector<bool>({false, false, true, true}));

  // clang-format off
  const auto partition_1_expected_data = std::vector<VariablySizedGroupRun::BlobDataType>{
    chars_to_blob_element("hell"), chars_to_blob_element("owor"), chars_to_blob_element("ldwh"), chars_to_blob_element("y"), // NOLINT
    chars_to_blob_element("grea"), chars_to_blob_element("tbad"), chars_to_blob_element("go"),
  };
  // clang-format on
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.data, partition_1_expected_data);
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.group_end_offsets, std::vector<size_t>({4, 7}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5, 10, 14, 6, 9, 13}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.null_values, std::vector<bool>({false, false, false, false}));
}

TEST_F(AggregateHashSortStepsTest, HashingFixed) {
  auto groups_layout = FixedSizeGroupRunLayout{2, {0, 2}};
  auto groups = FixedSizeGroupRun{&groups_layout};
  groups.resize(4);

  // clang-format off
  groups.hashes[0] = size_t{12}; groups.data[0] = int32_t{5}; groups.data[1] = int32_t{3};
  groups.hashes[1] = size_t{13}; groups.data[2] = int32_t{2}; groups.data[3] = int32_t{2};
  groups.hashes[2] = size_t{12}; groups.data[4] = int32_t{5}; groups.data[5] = int32_t{3};
  groups.hashes[3] = size_t{12}; groups.data[6] = int32_t{5}; groups.data[7] = int32_t{4};
  // clang-format on

  auto aggregate = std::make_unique<SumAggregateRun<int32_t>>();
  // clang-format off
  aggregate->values =      {5,     6,     7,     0};
  aggregate->null_values = {false, false, false, true};
  // clang-format on
  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>();
  aggregates.emplace_back(std::move(aggregate));

  auto run = opossum::aggregate_hashsort::Run<FixedSizeGroupRun>{std::move(groups), std::move(aggregates)};
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
  ASSERT_EQ(partitions.at(0).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_0 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(0).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_0);
  EXPECT_EQ(aggregate_0->values, std::vector<int64_t>({12, 0}));
  EXPECT_EQ(aggregate_0->null_values, std::vector<bool>({false, true}));

  ASSERT_EQ(partitions.at(1).runs.at(0).groups.data, std::vector<uint32_t>({2, 2}));
  ASSERT_EQ(partitions.at(1).runs.at(0).groups.hashes, std::vector<size_t>({13}));
  ASSERT_EQ(partitions.at(1).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_1 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(1).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_1);
  EXPECT_EQ(aggregate_1->values, std::vector<int64_t>({6}));
  EXPECT_EQ(aggregate_1->null_values, std::vector<bool>({false}));
}

TEST_F(AggregateHashSortStepsTest, HashingVariablySized) {
  // clang-format off
  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>{
    {true, ColumnID{0}, std::nullopt},
  };
  // clang-format on

  auto fixed_layout = FixedSizeGroupRunLayout{0, {}};
  const auto layout = VariablySizedGroupRunLayout{{ColumnID{0}}, {}, column_mapping, fixed_layout};
  auto groups = VariablySizedGroupRun{&layout};

  // clang-format off
  groups.data = {
  chars_to_blob_element("hell"), chars_to_blob_element("o"),
  chars_to_blob_element("worl"), chars_to_blob_element("d"),
  chars_to_blob_element("worl"), chars_to_blob_element("d"),
  };
  groups.group_end_offsets = {2, 4, 6};
  groups.value_end_offsets = {
    5,
    5,
    5
  };
  groups.null_values = {};
  groups.fixed.hashes = {0b0, 0b1, 0b1};
  // clang-format on

  auto partitioning = Partitioning{2, 0, 1};
  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>();

  auto run = opossum::aggregate_hashsort::Run<VariablySizedGroupRun>{std::move(groups), std::move(aggregates)};
  auto runs = std::vector<opossum::aggregate_hashsort::Run<VariablySizedGroupRun>>{};
  runs.emplace_back(std::move(run));

  AggregateHashSortConfig config;
  config.max_partitioning_counter = 100; // Partition the entire run

  const auto [continue_hashing, partitions] = hashing(config, std::move(runs), partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 1);
  EXPECT_EQ(run_offset, 0);
  EXPECT_TRUE(continue_hashing);

  ASSERT_EQ(partitions.size(), 2u);
  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  // clang-format off
  const auto partition_0_expected_data = std::vector<VariablySizedGroupRun::BlobDataType >{
  chars_to_blob_element("hell"), chars_to_blob_element("o"),
  };
  // clang-format on

  EXPECT_EQ(partitions.at(0).runs.at(0).groups.data, partition_0_expected_data);
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.group_end_offsets, std::vector<size_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.null_values, std::vector<bool>({}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.fixed.hashes, std::vector<size_t>({0b0}));

  // clang-format off
  const auto partition_1_expected_data = std::vector<VariablySizedGroupRun::BlobDataType >{
  chars_to_blob_element("worl"), chars_to_blob_element("d"),
  };
  // clang-format on

  EXPECT_EQ(partitions.at(1).runs.at(0).groups.data, partition_1_expected_data);
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.group_end_offsets, std::vector<size_t>({2}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.null_values, std::vector<bool>({}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.fixed.hashes, std::vector<size_t>({0b1}));
}

TEST_F(AggregateHashSortStepsTest, AggregateAdaptive) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {0}};
  auto groups = FixedSizeGroupRun{&groups_layout};
  groups.resize(8);

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
