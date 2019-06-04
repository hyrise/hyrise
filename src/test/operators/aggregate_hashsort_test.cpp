#include "base_test.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/aggregate_hashsort_steps.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::aggregate_hashsort;  // NOLINT
using namespace std::string_literals;  // NOLINT

namespace {

template<typename ElementType, typename... Args>
void add_group_data(std::vector<ElementType>& data, const Args&&... args) {
  auto offset = data.size() * sizeof(ElementType);

  const auto get_value_size = [&](const auto& value) {
    using VALUE_TYPE = std::decay_t<decltype(value)>;

    if constexpr (std::is_same_v<VALUE_TYPE, bool>) {
      return 1;
    } else if constexpr (std::is_same_v<VALUE_TYPE, std::string>) {
      return value.size();
    } else if constexpr (std::is_arithmetic_v<VALUE_TYPE>) {
      return sizeof(VALUE_TYPE);
    } else {
      Fail("Unexpected type");
    }
  };

  auto group_size = divide_and_ceil((get_value_size(args) + ...), sizeof(ElementType));

  data.resize(data.size() + group_size);

  const auto append = [&](const auto& value) {
    using VALUE_TYPE = std::decay_t<decltype(value)>;

    if constexpr (std::is_same_v<VALUE_TYPE, bool>) {
      auto bool_byte = static_cast<char>(value ? 1 : 0);
      memcpy(reinterpret_cast<char*>(data.data()) + offset, &bool_byte, 1);
      offset += 1;
    } else if constexpr (std::is_same_v<VALUE_TYPE, std::string>) {
      memcpy(reinterpret_cast<char*>(data.data()) + offset, value.data(), value.size());
      offset += value.size();
    } else if constexpr (std::is_arithmetic_v<VALUE_TYPE>){
      constexpr auto VALUE_SIZE = sizeof(std::decay_t<decltype(value)>);
      memcpy(reinterpret_cast<char*>(data.data()) + offset, &value, VALUE_SIZE);
      offset += VALUE_SIZE;
    } else {
      Fail("Unexpected type");
    }
  };

  (append(args), ...);
}

VariablySizedGroupRun::DataElementType chars_to_blob_element(const std::string& chars) {
  Assert(chars.size() <= sizeof(VariablySizedGroupRun::DataElementType), "Invalid string size");

  auto blob_element = VariablySizedGroupRun::DataElementType{};
  memcpy(&blob_element, chars.data(), chars.size());
  return blob_element;
}

}  // namespace

namespace opossum {

class AggregateHashSortTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::String, true},
                                                           {"b", DataType::Int, true},
                                                           {"c", DataType::String, false},
                                                           {"d", DataType::String, true},
                                                           {"e", DataType::Int, false},
                                                           {"f", DataType::Long, false}};
    table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

    // clang-format off
    table->append({"x",         11,          "abcd", "bcdef",     0, int64_t{12}});
    table->append({"y",         13,          "abcd", "bcdef",     1, int64_t{14}});

    table->append({"xy",        NullValue{}, "iiii", NullValue{}, 2, int64_t{15}});
    table->append({NullValue{}, NullValue{}, "",     NullValue{}, 3, int64_t{16}});

    table->append({"jjj",       NullValue{}, "ccc",  "oof",       4, int64_t{17}});
    table->append({"aa",        NullValue{}, "zzz",  NullValue{}, 5, int64_t{18}});

    table->append({"aa",        NullValue{}, "zyx",  NullValue{}, 6, int64_t{19}});
    table->append({"bb",        NullValue{}, "ddd",  NullValue{}, 7, int64_t{20}});
    // clang-format off
  }

  std::shared_ptr<Table> table;
};

TEST_F(AggregateHashSortTest, FixedSizeGroupRun) {
  auto layout = FixedSizeGroupRunLayout(6u, {ColumnID{0}, std::nullopt, ColumnID{1}}, {0, 9, 17});
  auto run = FixedSizeGroupRun{&layout, 0};

  add_group_data(run.data, false, int64_t{1}, int64_t{2}, false, int32_t{3});
  add_group_data(run.data, false, int64_t{4}, int64_t{5}, true, int32_t{0});
  add_group_data(run.data, true,  int64_t{1}, int64_t{2}, false, int32_t{3});
  add_group_data(run.data, true,  int64_t{0}, int64_t{8}, true, int32_t{0});
  add_group_data(run.data, false, int64_t{1}, int64_t{2}, false, int32_t{3});

  run.hashes = {0u, 1u, 2u, 3u, 4u};
  run.end = 5u;

  EXPECT_FALSE(run.compare(0, run, 2));
  EXPECT_FALSE(run.compare(0, run, 3));
  EXPECT_TRUE(run.compare(0, run, 4));

  const auto actual_segment_a = run.materialize_output<int64_t>(ColumnID{0}, true);
  const auto expected_segment_a = std::make_shared<ValueSegment<int64_t>>(std::vector<int64_t>{1, 4, 1, 0, 1}, std::vector<bool>{false, false, true, true, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_a, expected_segment_a);

  const auto actual_segment_b = run.materialize_output<int64_t>(ColumnID{1}, false);
  const auto expected_segment_b = std::make_shared<ValueSegment<int64_t>>(std::vector<int64_t>{2, 5, 2, 8, 2});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_b, expected_segment_b);

  const auto actual_segment_c = run.materialize_output<int32_t>(ColumnID{2}, true);
  const auto expected_segment_c = std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>{3, 0, 3, 0, 3}, std::vector<bool>{false, true, false, true, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_c, expected_segment_c);
}

TEST_F(AggregateHashSortTest, VariablySizedGroupRun) {
  const auto fixed_size_layout = FixedSizeGroupRunLayout{1, {}, {0}};

  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>{
    {true, ColumnID{0}, ColumnID{0}},
    {false, ColumnID{0}, std::nullopt},
    {true, ColumnID{1}, ColumnID{1}},
  };

  auto variably_sized_layout = VariablySizedGroupRunLayout{std::vector({ColumnID{2}, ColumnID{1}}), std::vector({ColumnID{0}}), column_mapping, fixed_size_layout};

  auto variably_sized_run = VariablySizedGroupRun{&variably_sized_layout, 0u, 0u};

  add_group_data(variably_sized_run.data, false, "aaa"s, false, "bbbb"s);
  add_group_data(variably_sized_run.data, false, "aaa"s, false, "bbbb"s);
  add_group_data(variably_sized_run.data, false, "ccccc"s, true, ""s);
  add_group_data(variably_sized_run.data, true,  ""s, false, "d"s);
  variably_sized_run.group_end_offsets = {3, 6, 8, 9};
  variably_sized_run.value_end_offsets = {4, 9, 4, 9, 6, 7, 1, 3};
  add_group_data(variably_sized_run.fixed.data, int32_t{0});
  add_group_data(variably_sized_run.fixed.data, int32_t{0});
  add_group_data(variably_sized_run.fixed.data, int32_t{1});
  add_group_data(variably_sized_run.fixed.data, int32_t{1});
  variably_sized_run.fixed.end = 4;

  EXPECT_TRUE(variably_sized_run.compare(0, variably_sized_run, 1));
  EXPECT_FALSE(variably_sized_run.compare(0, variably_sized_run, 2));

  const auto actual_segment_a = variably_sized_run.materialize_output<pmr_string>(ColumnID{0}, true);
  const auto expected_segment_a = std::make_shared<ValueSegment<pmr_string>>(std::vector<pmr_string>{"aaa", "aaa", "ccccc", ""}, std::vector<bool>{false, false, false, true});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_a, expected_segment_a);

  const auto actual_segment_b = variably_sized_run.materialize_output<pmr_string>(ColumnID{2}, true);
  const auto expected_segment_b = std::make_shared<ValueSegment<pmr_string>>(std::vector<pmr_string>{"bbbb", "bbbb", "", "d"}, std::vector<bool>{false, false, true, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_b, expected_segment_b);

}

TEST_F(AggregateHashSortTest, ProduceInitialGroupsFixed) {
  const auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});
  table->append({1.5f, 14, NullValue{}});

  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{1}, ColumnID{0}, ColumnID{2}};

  const auto layout = produce_initial_groups_layout<FixedSizeGroupRunLayout>(*table, group_by_column_ids);
  EXPECT_EQ(layout.group_size, 5u);
  EXPECT_EQ(layout.column_base_offsets, std::vector<size_t>({0, 4, 9}));
  EXPECT_EQ(layout.nullable_column_count, 2);
  EXPECT_EQ(layout.nullable_column_indices, std::vector<std::optional<ColumnID>>({std::nullopt, ColumnID{0}, ColumnID{1}}));

  const auto [groups, end_row_id] = produce_initial_groups(table, &layout, group_by_column_ids, RowID{ChunkID{0}, ChunkOffset{1}}, 3);

  EXPECT_EQ(end_row_id, RowID(ChunkID{0}, ChunkOffset{4}));
  EXPECT_EQ(groups.hashes.size(), 3u);
  EXPECT_EQ(groups.end, 3u);

  // clang-format off
  auto expected_group_data = std::vector<FixedSizeGroupRun::DataElementType>{};
  add_group_data(expected_group_data, int32_t{13}, false, 4.5f, false, 1.0);
  add_group_data(expected_group_data, int32_t{12}, true, 0.0f, false, -2.0);
  add_group_data(expected_group_data, int32_t{14}, false, 1.5f, true, 0.0);
  // clang-format on

  EXPECT_EQ(groups.data, expected_group_data);
}

TEST_F(AggregateHashSortTest, ProduceInitialGroupsLayoutVariablySized) {
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
  EXPECT_EQ(layout.fixed_layout.group_size, 4u);
  EXPECT_EQ(layout.fixed_layout.column_base_offsets, std::vector<size_t>({0, 5}));
  EXPECT_EQ(layout.fixed_layout.nullable_column_indices, std::vector<std::optional<ColumnID>>({ColumnID{}, std::nullopt}));
}

TEST_F(AggregateHashSortTest, ProduceInitialGroupsVariablySizedDataBudgetLimited) {
  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5}};

  const auto layout = produce_initial_groups_layout<VariablySizedGroupRunLayout>(*table, group_by_column_ids);

  // Produce groups with data/row budget so that the data budget per column (12 * uint32_t) gets exhausted first, after
  // 4 rows
  const auto [groups, end_row_id] = produce_initial_groups(table, &layout, group_by_column_ids, RowID{ChunkID{0}, ChunkOffset{1}}, 100, 12);

  EXPECT_EQ(end_row_id, RowID(ChunkID{2}, ChunkOffset{1}));

  // clang-format off
  auto expected_variably_sized_group_data = std::vector<VariablySizedGroupRun::DataElementType>();
  add_group_data(expected_variably_sized_group_data, false, "y"s, "abcd"s, false, "bcdef"s);
  add_group_data(expected_variably_sized_group_data, false, "xy"s, "iiii"s, true, ""s);
  add_group_data(expected_variably_sized_group_data, true, ""s, ""s, true, ""s);
  add_group_data(expected_variably_sized_group_data, false, "jjj"s, "ccc"s, false, "oof"s);
  // clang-format on

  EXPECT_EQ(groups.data, expected_variably_sized_group_data);

  // clang-format off
  EXPECT_EQ(groups.group_end_offsets, std::vector<size_t>({3, 5, 6, 9}));

  EXPECT_EQ(groups.value_end_offsets, std::vector<size_t>({
    2, 6, 12,
    3, 7, 8,
    1, 1, 2,
    4, 7, 11
  }));
  // clang-format on

  EXPECT_EQ(groups.fixed.hashes.size(), 4u);

  // clang-format off
  auto expected_fixed_group_data = std::vector<FixedSizeGroupRun::DataElementType>{};
  add_group_data(expected_fixed_group_data, false, 13, int64_t{14});
  add_group_data(expected_fixed_group_data, true, 0, int64_t{15});
  add_group_data(expected_fixed_group_data, true, 0, int64_t{16});
  add_group_data(expected_fixed_group_data, true, 0, int64_t{17});
  // clang-format on

  EXPECT_EQ(groups.fixed.data, expected_fixed_group_data);
  EXPECT_EQ(groups.fixed.end, 4);
}

TEST_F(AggregateHashSortTest, ProduceInitialGroupsVariablySizedRowBudgetLimited) {
  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{2}, ColumnID{3}};

  const auto layout = produce_initial_groups_layout<VariablySizedGroupRunLayout>(*table, group_by_column_ids);

  // Produce groups with data/row budget so that the row budget gets exhausted before the group data budget
  const auto [groups, end_row_id] = produce_initial_groups(table, &layout, group_by_column_ids, RowID{ChunkID{0}, ChunkOffset{1}}, 4, 50);

  EXPECT_EQ(end_row_id, RowID(ChunkID{2}, ChunkOffset{1}));

  // clang-format off
  auto expected_variably_sized_group_data = std::vector<VariablySizedGroupRun::DataElementType>();
  add_group_data(expected_variably_sized_group_data, false, "y"s, "abcd"s, false, "bcdef"s);
  add_group_data(expected_variably_sized_group_data, false, "xy"s, "iiii"s, true, ""s);
  add_group_data(expected_variably_sized_group_data, true, ""s, ""s, true, ""s);
  add_group_data(expected_variably_sized_group_data, false, "jjj"s, "ccc"s, false, "oof"s);
  // clang-format on

  EXPECT_EQ(groups.data, expected_variably_sized_group_data);

  // clang-format off
  EXPECT_EQ(groups.group_end_offsets, std::vector<size_t>({3, 5, 6, 9}));

  EXPECT_EQ(groups.value_end_offsets, std::vector<size_t>({
    2, 6, 12,
    3, 7, 8,
    1, 1, 2,
    4, 7, 11
  }));
  // clang-format on

  EXPECT_EQ(groups.fixed.hashes.size(), 4u);
  EXPECT_EQ(groups.fixed.data, std::vector<FixedSizeGroupRun::DataElementType>());
  EXPECT_EQ(groups.fixed.end, 4);
}

TEST_F(AggregateHashSortTest, ProduceInitialGroupsVariablySizedTableSizeLimitted) {
  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{2}, ColumnID{3}};

  const auto layout = produce_initial_groups_layout<VariablySizedGroupRunLayout>(*table, group_by_column_ids);

  // Produce groups with data/row budget so that the row budget gets exhausted before the group data budget
  const auto [groups, end_row_id] = produce_initial_groups(table, &layout, group_by_column_ids, RowID{ChunkID{3}, ChunkOffset{1}}, 50, 100);

  EXPECT_EQ(end_row_id, RowID(ChunkID{3}, ChunkOffset{1}));

  // clang-format off
  auto expected_variably_sized_group_data = std::vector<VariablySizedGroupRun::DataElementType>();
  add_group_data(expected_variably_sized_group_data, false, "bb"s, "ddd"s, true, ""s);
  // clang-format on

  EXPECT_EQ(groups.data, expected_variably_sized_group_data);
  // clang-format off

  EXPECT_EQ(groups.group_end_offsets, std::vector<size_t>({2}));
  EXPECT_EQ(groups.value_end_offsets, std::vector<size_t>({3, 6, 7}));
  // clang-format on

  EXPECT_EQ(groups.fixed.hashes.size(), 1u);
  EXPECT_EQ(groups.fixed.data, std::vector<uint32_t>());
  EXPECT_EQ(groups.fixed.end, 1);
}

TEST_F(AggregateHashSortTest, ProduceInitialAggregates) {
  auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});
  table->append({1.5f, 14, NullValue{}});

  auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{
      {std::nullopt, AggregateFunction::CountRows},
      {ColumnID{0}, AggregateFunction::Sum},
      {ColumnID{1}, AggregateFunction::Min},
      {ColumnID{2}, AggregateFunction::Avg},
  };

  auto aggregates = produce_initial_aggregates(table, aggregate_column_definitions, false, RowID{ChunkID{0}, ChunkOffset{1}}, 3);

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
  const auto expected_avg_aggregates = std::vector<std::pair<double, size_t>>({{1.0, 1}, {-2.0, 1}, {0.0, 0}});
  EXPECT_EQ(avg->pairs, expected_avg_aggregates);
}

TEST_F(AggregateHashSortTest, Partitioning) {
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(1), 1);
  EXPECT_EQ(Partitioning(2, 0, 1).get_partition_index(2), 0);
}

TEST_F(AggregateHashSortTest, PartitionFixedOnly) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {std::nullopt}, {0}};
  auto groups = FixedSizeGroupRun{&groups_layout, 7};

  // clang-format off
  groups.hashes[0] = size_t{0b001}; groups.data[0] = int32_t{5};
  groups.hashes[1] = size_t{0b100}; groups.data[1] = int32_t{2};
  groups.hashes[2] = size_t{0b101}; groups.data[2] = int32_t{5};
  groups.hashes[3] = size_t{0b110}; groups.data[3] = int32_t{6};
  groups.hashes[4] = size_t{0b000}; groups.data[4] = int32_t{7};
  groups.hashes[5] = size_t{0b100}; groups.data[5] = int32_t{8};
  groups.hashes[6] = size_t{0b100}; groups.data[6] = int32_t{8};
  groups.end = 7;
  // clang-format off

  auto aggregate = std::make_unique<SumAggregateRun<int32_t>>(7);
  // clang-format off
  aggregate->values =      {0,     1,     2,     3,     0,    5,     6};
  aggregate->null_values = {false, false, false, false, true, false, false};
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

  auto run_source = PartitionRunSource<FixedSizeGroupRun>{std::move(runs)};
  const auto partitions = partition(config, run_source, partitioning, run_idx, run_offset);
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

TEST_F(AggregateHashSortTest, PartitionVariablySizedAndFixed) {
  // clang-format off
  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>{
    {true, ColumnID{0}, std::nullopt},
    {false, ColumnID{0}, std::nullopt},
    {true, ColumnID{1}, 0},
    {false, ColumnID{1}, std::nullopt},
    {true, ColumnID{2}, 1},
  };
  // clang-format on

  auto fixed_layout = FixedSizeGroupRunLayout{0, {}, {}};
  const auto layout = VariablySizedGroupRunLayout{{ColumnID{0}, ColumnID{1}, ColumnID{2}}, {}, column_mapping, fixed_layout};
  auto groups = VariablySizedGroupRun{&layout, 4, 36};
  groups.fixed.end = 4;

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
//  groups.null_values = {
//    false, false,
//    false, false,
//    true, true,
//    false, false
//  };
  groups.fixed.hashes = {0b1, 0b0, 0b0, 0b1};
  groups.fixed.end = 4;
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

  auto run_source = PartitionRunSource<VariablySizedGroupRun>(std::move(runs));
  const auto partitions = partition(config, run_source, partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 1);
  EXPECT_EQ(run_offset, 0);

  ASSERT_EQ(partitions.size(), 2);

  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  // clang-format off
  const auto partition_0_expected_data = std::vector<VariablySizedGroupRun::DataElementType >{
    chars_to_blob_element("yesn"), chars_to_blob_element("omay"), chars_to_blob_element("be"),
    chars_to_blob_element("yet")
  };
  // clang-format on
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.data, partition_0_expected_data);
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.group_end_offsets, std::vector<size_t>({3, 4}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.value_end_offsets, std::vector<size_t>({4, 6, 11, 4, 4, 4}));
//  EXPECT_EQ(partitions.at(0).runs.at(0).groups.null_values, std::vector<bool>({false, false, true, true}));

  // clang-format off
  const auto partition_1_expected_data = std::vector<VariablySizedGroupRun::DataElementType>{
    chars_to_blob_element("hell"), chars_to_blob_element("owor"), chars_to_blob_element("ldwh"), chars_to_blob_element("y"), // NOLINT
    chars_to_blob_element("grea"), chars_to_blob_element("tbad"), chars_to_blob_element("go"),
  };
  // clang-format on
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.data, partition_1_expected_data);
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.group_end_offsets, std::vector<size_t>({4, 7}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5, 10, 14, 6, 9, 13}));
//  EXPECT_EQ(partitions.at(1).runs.at(0).groups.null_values, std::vector<bool>({false, false, false, false}));
}

TEST_F(AggregateHashSortTest, HashingFixed) {
  auto groups_layout = FixedSizeGroupRunLayout{2, {}, {0, 2}};
  auto groups = FixedSizeGroupRun{&groups_layout, 4};

  // clang-format off
  groups.hashes[0] = size_t{12}; groups.data[0] = int32_t{5}; groups.data[1] = int32_t{3};
  groups.hashes[1] = size_t{13}; groups.data[2] = int32_t{2}; groups.data[3] = int32_t{2};
  groups.hashes[2] = size_t{12}; groups.data[4] = int32_t{5}; groups.data[5] = int32_t{3};
  groups.hashes[3] = size_t{12}; groups.data[6] = int32_t{5}; groups.data[7] = int32_t{4};
  groups.end = 4;
  // clang-format on

  auto aggregate = std::make_unique<SumAggregateRun<int32_t>>(4);
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

  auto run_source = PartitionRunSource<FixedSizeGroupRun>{std::move(runs)};
  const auto [continue_hashing, partitions] = hashing(config, run_source, partitioning, run_idx, run_offset);
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

TEST_F(AggregateHashSortTest, HashingVariablySized) {
  // clang-format off
  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>{
    {true, ColumnID{0}, std::nullopt},
  };
  // clang-format on

  auto fixed_layout = FixedSizeGroupRunLayout{0, {}, {}};
  const auto layout = VariablySizedGroupRunLayout{{ColumnID{0}}, {}, column_mapping, fixed_layout};
  auto groups = VariablySizedGroupRun{&layout, 3, 15};
  groups.fixed.end = 3;

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

  auto run_source = PartitionRunSource<VariablySizedGroupRun>{std::move(runs)};
  const auto [continue_hashing, partitions] = hashing(config, run_source, partitioning, run_idx, run_offset);
  EXPECT_EQ(run_idx, 1);
  EXPECT_EQ(run_offset, 0);
  EXPECT_TRUE(continue_hashing);

  ASSERT_EQ(partitions.size(), 2u);
  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  // clang-format off
  const auto partition_0_expected_data = std::vector<VariablySizedGroupRun::DataElementType >{
  chars_to_blob_element("hell"), chars_to_blob_element("o"),
  };
  // clang-format on

  EXPECT_EQ(partitions.at(0).runs.at(0).groups.data, partition_0_expected_data);
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.group_end_offsets, std::vector<size_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5}));
  EXPECT_EQ(partitions.at(0).runs.at(0).groups.fixed.hashes, std::vector<size_t>({0b0}));

  // clang-format off
  const auto partition_1_expected_data = std::vector<VariablySizedGroupRun::DataElementType >{
  chars_to_blob_element("worl"), chars_to_blob_element("d"),
  };
  // clang-format on

  EXPECT_EQ(partitions.at(1).runs.at(0).groups.data, partition_1_expected_data);
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.group_end_offsets, std::vector<size_t>({2}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.value_end_offsets, std::vector<size_t>({5}));
  EXPECT_EQ(partitions.at(1).runs.at(0).groups.fixed.hashes, std::vector<size_t>({0b1}));
}

TEST_F(AggregateHashSortTest, AggregateAdaptive) {
  auto groups_layout = FixedSizeGroupRunLayout{1, {std::nullopt}, {0}};
  auto groups = FixedSizeGroupRun{&groups_layout, 8};

  // clang-format off
  groups.hashes[0] = size_t{0b1}; groups.data[0] = int32_t{5};
  groups.hashes[1] = size_t{0b1}; groups.data[1] = int32_t{3};
  groups.hashes[2] = size_t{0b1}; groups.data[2] = int32_t{5};
  groups.hashes[3] = size_t{0b0}; groups.data[3] = int32_t{2};
  groups.hashes[4] = size_t{0b1}; groups.data[4] = int32_t{3};
  groups.hashes[5] = size_t{0b1}; groups.data[5] = int32_t{5};
  groups.hashes[6] = size_t{0b0}; groups.data[6] = int32_t{2};
  groups.hashes[7] = size_t{0b0}; groups.data[7] = int32_t{2};
  groups.end = 8;
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

  auto run_source = std::make_unique<PartitionRunSource<FixedSizeGroupRun>>(std::move(runs));
  const auto partitions = adaptive_hashing_and_partition<FixedSizeGroupRun>(config, std::move(run_source), partitioning);

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
