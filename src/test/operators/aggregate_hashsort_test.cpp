#include "base_test.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/aggregate_hashsort_algorithm.hpp"
#include "operators/aggregate/aggregate_hashsort_utils.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum;                      // NOLINT
using namespace opossum::aggregate_hashsort;  // NOLINT
using namespace std::string_literals;         // NOLINT

namespace {

template <typename T>
struct IsOptional {
  static inline constexpr auto value = false;
};
template <typename T>
struct IsOptional<std::optional<T>> {
  static inline constexpr auto value = true;
};
template <typename T>
constexpr auto is_optional_v = IsOptional<T>::value;

template <typename T>
constexpr auto is_variably_sized_v = std::is_same_v<std::string, T> || std::is_same_v<std::optional<std::string>, T>;

template <typename T>
size_t get_value_size(const T& value) {
  using VALUE_TYPE = std::decay_t<decltype(value)>;

  if constexpr (is_optional_v<VALUE_TYPE>) {
    return 1 + get_value_size(value ? *value : typename VALUE_TYPE::value_type{});
  } else if constexpr (std::is_same_v<VALUE_TYPE, std::string>) {
    return value.size();
  } else if constexpr (std::is_arithmetic_v<VALUE_TYPE>) {
    return sizeof(VALUE_TYPE);
  } else {
    Fail("Unexpected type");
  }
}

/**
 * Utility function to build a group as a binary blob
 */
template <typename Run, typename... Args>
void add_group(Run& run, const Args&&... args) {
  auto& data = run.group_data;

  auto offset = data.size() * sizeof(GroupRunElementType);

  const auto group_data_byte_count = (get_value_size(args) + ...);
  const auto group_meta_data_byte_count = ((is_variably_sized_v<Args> ? sizeof(size_t) : 0) + ...);
  const auto group_size =
      divide_and_ceil(group_data_byte_count + group_meta_data_byte_count, sizeof(GroupRunElementType));
  const auto old_size = data.size();

  data.resize(data.size() + group_size);
  std::fill(data.begin() + old_size, data.end(), 0);

  auto variably_sized_values_begin_offset = size_t{};
  auto variably_sized_value_end_offsets = std::vector<size_t>{};

  const auto write_value = [&](const auto& value) {
    using VALUE_TYPE = std::decay_t<decltype(value)>;

    if constexpr (std::is_same_v<VALUE_TYPE, std::string>) {
      memcpy(reinterpret_cast<char*>(data.data()) + offset, value.data(), value.size());
      offset += value.size();

    } else if constexpr (std::is_arithmetic_v<VALUE_TYPE>) {
      constexpr auto VALUE_SIZE = sizeof(std::decay_t<decltype(value)>);
      memcpy(reinterpret_cast<char*>(data.data()) + offset, &value, VALUE_SIZE);
      offset += VALUE_SIZE;

    } else {
      Fail("Unexpected type");
    }
  };

  const auto for_each_value = [&](const auto& value) {
    using VALUE_TYPE = std::decay_t<decltype(value)>;

    // Gather meta info about variably-sized value
    if constexpr (is_variably_sized_v<VALUE_TYPE>) {
      if (variably_sized_value_end_offsets.empty()) {
        variably_sized_value_end_offsets.emplace_back(variably_sized_values_begin_offset + get_value_size(value));
      } else {
        variably_sized_value_end_offsets.emplace_back(variably_sized_value_end_offsets.back() + get_value_size(value));
      }
    } else {
      variably_sized_values_begin_offset += get_value_size(value);
    }

    // Write out values
    if constexpr (is_optional_v<VALUE_TYPE>) {
      // Write is_null
      *(reinterpret_cast<char*>(data.data()) + offset) = value ? 0 : 1;
      offset += 1;

      write_value(value.value_or(typename VALUE_TYPE::value_type{}));

    } else if constexpr (std::is_same_v<VALUE_TYPE, std::string>) {
      memcpy(reinterpret_cast<char*>(data.data()) + offset, value.data(), value.size());
      offset += value.size();

    } else if constexpr (std::is_arithmetic_v<VALUE_TYPE>) {
      constexpr auto VALUE_SIZE = sizeof(std::decay_t<decltype(value)>);
      memcpy(reinterpret_cast<char*>(data.data()) + offset, &value, VALUE_SIZE);
      offset += VALUE_SIZE;
    } else {
      Fail("Unexpected type");
    }
  };

  // Write data
  (for_each_value(args), ...);

  // Optionally, write meta data about variably-sized values aligned to the *end* of the binary blob
  auto* value_end_offsets_target =
      reinterpret_cast<char*>(&data[data.size()]) - sizeof(size_t) * variably_sized_value_end_offsets.size();
  memcpy(value_end_offsets_target, variably_sized_value_end_offsets.data(),
         sizeof(size_t) * variably_sized_value_end_offsets.size());

  // Add run metadata
  ++run.size;
  run.hashes.emplace_back(0);

  if constexpr (std::is_same_v<typename Run::GroupSizePolicyType, VariableGroupSizePolicy>) {
    run.data_watermark = run.group_data.size();
    run.group_end_offsets.emplace_back(run.group_data.size());
  }
}

}  // namespace

namespace opossum {

class AggregateHashSortTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto column_definitions = TableColumnDefinitions{
        {"a", DataType::String, true}, {"b", DataType::Int, true},  {"c", DataType::String, false},
        {"d", DataType::String, true}, {"e", DataType::Int, false}, {"f", DataType::Long, false},
        {"g", DataType::Double, false}};
    table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

    // clang-format off
    table->append({"x",         11,          "abcd", "bcdef",     0, int64_t{12}, 10.0});
    table->append({"y",         13,          "abcd", "bcdef",     1, int64_t{14}, 9.0});

    table->append({"xy",        NullValue{}, "iiii", NullValue{}, 2, int64_t{15}, 8.0});
    table->append({NullValue{}, NullValue{}, "",     NullValue{}, 3, int64_t{16}, 7.0});

    table->append({"jjj",       NullValue{}, "ccc",  "oof",       4, int64_t{17}, 6.0});
    table->append({"aa",        NullValue{}, "zzz",  NullValue{}, 5, int64_t{18}, 5.0});

    table->append({"aa",        NullValue{}, "zyx",  NullValue{}, 6, int64_t{19}, 4.0});
    table->append({"bb",        NullValue{}, "ddd",  NullValue{}, 7, int64_t{20}, 3.0});
    // clang-format off
  }

  std::shared_ptr<Table> table;
};

TEST_F(AggregateHashSortTest, Definition) {
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                         {"b", DataType::String, false},
                                                         {"c", DataType::String, true},
                                                         {"d", DataType::Int, false},
                                                         {"e", DataType::Long, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{4}};
  const auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{
  {std::nullopt, AggregateFunction::CountRows},
  {ColumnID{1}, AggregateFunction::CountDistinct},
  };

  const auto setup = AggregateHashSortSetup::create({}, table, aggregate_column_definitions, group_by_column_ids);

  EXPECT_EQ(setup.offsets, std::vector<size_t>({0, 0, 1, 5}));
  EXPECT_EQ(setup.variably_sized_column_ids, std::vector({ColumnID{1}, ColumnID{2}}));
  EXPECT_EQ(setup.fixed_size_column_ids, std::vector({ColumnID{0}, ColumnID{4}}));
  EXPECT_EQ(setup.fixed_size_value_offsets, std::vector<size_t>({0, 5}));
  EXPECT_EQ(setup.variably_sized_values_begin_offset, 13);
  EXPECT_EQ(setup.fixed_group_size, divide_and_ceil(size_t{13}, GROUP_RUN_ELEMENT_SIZE));
  EXPECT_EQ(setup.aggregate_definitions.size(), 2);
  EXPECT_EQ(setup.aggregate_definitions.at(0), AggregateHashSortAggregateDefinition(AggregateFunction::CountRows));
  EXPECT_EQ(setup.aggregate_definitions.at(1), AggregateHashSortAggregateDefinition(AggregateFunction::CountDistinct, DataType::String, ColumnID{1}));
}

TEST_F(AggregateHashSortTest, CreateRun) {
  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{4}};
  const auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{
  {std::nullopt, AggregateFunction::CountRows},
  {ColumnID{1}, AggregateFunction::CountDistinct},
  };

  const auto setup = AggregateHashSortSetup::create({}, table, aggregate_column_definitions, group_by_column_ids);

  const auto variable_group_size_run = create_run<BasicRun<VariableGroupSizePolicy>>(setup, size_t{3}, size_t{50});
  EXPECT_EQ(variable_group_size_run.group_data.size(), 50u);
  EXPECT_EQ(variable_group_size_run.hashes.size(), 3u);
  EXPECT_EQ(variable_group_size_run.group_end_offsets.size(), 3u);
}

TEST_F(AggregateHashSortTest, RunWithFixedGroupSize) {
  auto setup = AggregateHashSortSetup{};
  setup.fixed_group_size = 6u;
  setup.offsets = {0, 9, 17};

  auto run = create_run<BasicRun<DynamicFixedGroupSizePolicy>>(setup);

  add_group(run, std::optional<int64_t>{1}, int64_t{2}, std::optional<int32_t>{3});
  add_group(run, std::optional<int64_t>{4}, int64_t{5}, std::optional<int32_t>{});
  add_group(run, std::optional<int64_t>{},  int64_t{2}, std::optional<int32_t>{3});
  add_group(run, std::optional<int64_t>{},  int64_t{8}, std::optional<int32_t>{});
  add_group(run, std::optional<int64_t>{1}, int64_t{2}, std::optional<int32_t>{3});

  run.hashes = {0u, 1u, 2u, 3u, 4u};

  EXPECT_EQ(run.make_key(0).hash, 0u);
  EXPECT_EQ(run.make_key(0).group, &run.group_data[0]);
  EXPECT_EQ(run.make_key(1).group, &run.group_data[6]);

  auto compare = DynamicFixedGroupSizePolicy::HashTableCompare{setup};

  EXPECT_FALSE(compare(run.make_key(0), run.make_key(2)));
  EXPECT_FALSE(compare(run.make_key(0), run.make_key(3)));
  EXPECT_TRUE(compare(run.make_key(0), run.make_key(4)));

  const auto actual_segment_a = run.materialize_group_column<int64_t>(setup, ColumnID{0}, true);
  const auto expected_segment_a = std::make_shared<ValueSegment<int64_t>>(std::vector<int64_t>{1, 4, 1, 0, 1}, std::vector<bool>{false, false, true, true, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_a, expected_segment_a);

  const auto actual_segment_b = run.materialize_group_column<int64_t>(setup, ColumnID{1}, false);
  const auto expected_segment_b = std::make_shared<ValueSegment<int64_t>>(std::vector<int64_t>{2, 5, 2, 8, 2});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_b, expected_segment_b);

  const auto actual_segment_c = run.materialize_group_column<int32_t>(setup, ColumnID{2}, true);
  const auto expected_segment_c = std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>{3, 0, 3, 0, 3}, std::vector<bool>{false, true, false, true, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_c, expected_segment_c);
}

TEST_F(AggregateHashSortTest, VariablySizedGroupRun) {
  // Four group-by columns: nullable_int, string, double, nullable_string

  const auto group_by_column_ids = std::vector{ColumnID{1}, ColumnID{2}, ColumnID{6}, ColumnID{0}};
  const auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);

  auto run = create_run<BasicRun<VariableGroupSizePolicy>>(setup);

  // clang-format off
  add_group(run, std::optional<int32_t>{1}, 3.0,  "abc"s, std::optional<std::string>{"defg"}); run.hashes.back() = 1; // NOLINT
  add_group(run, std::optional<int32_t>{2}, 14.0, "hij"s, std::optional<std::string>{});       run.hashes.back() = 2; // NOLINT
  add_group(run, std::optional<int32_t>{3}, 25.0, ""s,    std::optional<std::string>{});       run.hashes.back() = 3; // NOLINT
  add_group(run, std::optional<int32_t>{},  36.0, "z"s,   std::optional<std::string>{"supi"}); run.hashes.back() = 4; // NOLINT
  add_group(run, std::optional<int32_t>{1}, 3.0,  "abc"s, std::optional<std::string>{"defg"}); run.hashes.back() = 1; // NOLINT
  // clang-format off

  const auto initial_run_size = run.size;
  const auto initial_watermark = run.data_watermark;

  EXPECT_EQ(run.get_group_range(0), std::make_pair(size_t(0), size_t(10)));
  EXPECT_EQ(run.get_group_range(1), std::make_pair(size_t(10), size_t(9)));
  EXPECT_EQ(run.get_group_range(2), std::make_pair(size_t(19), size_t(8)));
  EXPECT_EQ(run.get_group_range(3), std::make_pair(size_t(27), size_t(9)));
  EXPECT_EQ(run.get_group_range(4), std::make_pair(size_t(36), size_t(10)));

  EXPECT_EQ(run.get_variably_sized_value_range(setup, 0, 0), std::make_pair(size_t(13), size_t(3)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 0, 1), std::make_pair(size_t(16), size_t(5)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 1, 0), std::make_pair(size_t(53), size_t(3)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 1, 1), std::make_pair(size_t(56), size_t(1)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 2, 0), std::make_pair(size_t(89), size_t(0)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 2, 1), std::make_pair(size_t(89), size_t(1)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 3, 0), std::make_pair(size_t(121), size_t(1)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 3, 1), std::make_pair(size_t(122), size_t(5)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 4, 0), std::make_pair(size_t(157), size_t(3)));
  EXPECT_EQ(run.get_variably_sized_value_range(setup, 4, 1), std::make_pair(size_t(160), size_t(5)));

  auto compare = VariablySizedCompare{setup};

  EXPECT_TRUE(compare(run.make_key(0), run.make_key(4)));
  EXPECT_FALSE(compare(run.make_key(0), run.make_key(1)));

  EXPECT_EQ(run.data_watermark, initial_watermark);

  run.append(run, 0, 2);
  EXPECT_EQ(run.data_watermark, initial_watermark + 10);
  EXPECT_EQ(run.size, initial_run_size);

  run.flush_buffers(run);
  EXPECT_EQ(run.size, initial_run_size + 1);

  EXPECT_TRUE(std::equal(run.group_data.begin(), run.group_data.begin() + 10, run.group_data.begin() + initial_watermark));
  EXPECT_EQ(run.group_end_offsets[run.size - 1], initial_watermark + 10);
  EXPECT_EQ(run.hashes[run.size - 1], 1);

  const auto actual_segment_a = run.materialize_group_column<int32_t>(setup, ColumnID{0}, true);
  const auto expected_segment_a = std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>{1, 2, 3, 0, 1, 1}, std::vector<bool>{false, false, false, true, false, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_a, expected_segment_a);

  const auto actual_segment_b = run.materialize_group_column<pmr_string>(setup, ColumnID{1}, false);
  const auto expected_segment_b = std::make_shared<ValueSegment<pmr_string>>(std::vector<pmr_string>{"abc", "hij", "", "z", "abc", "abc"});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_b, expected_segment_b);

  const auto actual_segment_c = run.materialize_group_column<double>(setup, ColumnID{2}, false);
  const auto expected_segment_c = std::make_shared<ValueSegment<double>>(std::vector<double>{3.0, 14.0, 25.0, 36.0, 3.0, 3.0});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_c, expected_segment_c);

  const auto actual_segment_d = run.materialize_group_column<pmr_string>(setup, ColumnID{3}, true);
  const auto expected_segment_d = std::make_shared<ValueSegment<pmr_string>>(std::vector<pmr_string>{"defg", "", "", "supi", "defg", "defg"}, std::vector<bool>{false, true, true, false, false, false});
  EXPECT_SEGMENT_EQ_ORDERED(actual_segment_d, expected_segment_d);
}

TEST_F(AggregateHashSortTest, TableRunSourceFixedOnly) {
  const auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});
  table->append({1.5f, 15, NullValue{}});
  table->append({NullValue{}, 13, -2.0});

  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{1}, ColumnID{0}, ColumnID{2}};

  auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);
  setup.config.initial_run_size = 3;

  EXPECT_EQ(setup.fixed_group_size, 5u);
  EXPECT_EQ(setup.offsets, std::vector<size_t>({0, 4, 9}));

  auto run_source = TableRunSource<BasicRun<DynamicFixedGroupSizePolicy>>{setup, table};
  run_source.fetch_run(setup);

  EXPECT_EQ(run_source.begin_row_id, RowID(ChunkID{0}, ChunkOffset{3}));

  EXPECT_EQ(run_source.remaining_fetched_group_count, 3u);
  EXPECT_EQ(run_source.remaining_fetched_group_data_size, 15u);
  EXPECT_EQ(run_source.current_run().hashes.size(), 3u);
  EXPECT_EQ(run_source.current_run().size, 3u);

  // clang-format off
  auto expected_group_run_a = create_run<BasicRun<DynamicFixedGroupSizePolicy>>(setup);
  add_group(expected_group_run_a, int32_t{13}, std::optional<float>{4.5f}, std::optional<double>{1.0});
  add_group(expected_group_run_a, int32_t{12}, std::optional<float>{},     std::optional<double>{-2.0});
  add_group(expected_group_run_a, int32_t{14}, std::optional<float>{1.5f}, std::optional<double>{});
  // clang-format on
  EXPECT_EQ(run_source.current_run().group_data, expected_group_run_a.group_data);

  EXPECT_EQ(run_source.run_idx, 0u);
  EXPECT_EQ(run_source.run_offset, 0u);

  run_source.next_group_in_run();
  EXPECT_EQ(run_source.run_idx, 0u);
  EXPECT_EQ(run_source.run_offset, 1u);
  EXPECT_EQ(run_source.remaining_fetched_group_count, 2u);
  EXPECT_EQ(run_source.remaining_fetched_group_data_size, 10u);

  run_source.next_group_in_run();
  run_source.next_group_in_run();
  EXPECT_EQ(run_source.run_idx, 0u);
  EXPECT_EQ(run_source.run_offset, 3u);
  EXPECT_TRUE(run_source.end_of_run());
  EXPECT_FALSE(run_source.end_of_source());

  run_source.next_run(setup);
  EXPECT_EQ(run_source.run_idx, 1u);
  EXPECT_EQ(run_source.run_offset, 0u);
  EXPECT_EQ(run_source.remaining_fetched_group_count, 2u);
  EXPECT_EQ(run_source.remaining_fetched_group_data_size, 10u);
  EXPECT_FALSE(run_source.end_of_run());
  EXPECT_FALSE(run_source.end_of_source());
  // clang-format off

  // clang-format off
  auto expected_group_run_b = create_run<BasicRun<DynamicFixedGroupSizePolicy>>(setup);
  add_group(expected_group_run_b, int32_t{15}, std::optional<float>{1.5f}, std::optional<double>{});
  add_group(expected_group_run_b, int32_t{13}, std::optional<float>{},     std::optional<double>{-2.0});
  // clang-format on
  EXPECT_EQ(run_source.current_run().group_data, expected_group_run_b.group_data);
}

TEST_F(AggregateHashSortTest, TableRunSourceMaterializeVariablySizedColumns) {
  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5}};
  const auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);

  const auto begin_row_id = RowID{ChunkID{0}, ChunkOffset{1}};
  const auto row_count = 4;
  const auto [data_per_column, value_end_offsets, end_row_id] = TableRunSource<BasicRun<VariableGroupSizePolicy>>::materialize_variably_sized_columns(setup, table,
  begin_row_id, row_count);

  EXPECT_EQ(end_row_id, RowID(ChunkID{2}, ChunkOffset{1}));
  EXPECT_EQ(data_per_column.size(), 3u);
  EXPECT_EQ(value_end_offsets.size(), 3u * 4u);

  EXPECT_EQ(value_end_offsets, uninitialized_vector<size_t>({15, 19, 25, 16, 20, 21, 14, 14, 15, 17, 20, 24}));

  EXPECT_EQ(data_per_column.at(0), uninitialized_vector<char>({'\0', 'y', '\0', 'x', 'y', '\1', '\0', 'j', 'j', 'j'}));
  EXPECT_EQ(data_per_column.at(1), uninitialized_vector<char>({'a', 'b', 'c', 'd', 'i', 'i', 'i', 'i', 'c', 'c', 'c'}));
  EXPECT_EQ(data_per_column.at(2), uninitialized_vector<char>({'\0', 'b', 'c', 'd', 'e', 'f', '\1', '\1', '\0', 'o', 'o', 'f'}));
}

TEST_F(AggregateHashSortTest, TableRunSourceDetermineGroupEndOffsets) {
  // Sizes in bytes
  const auto value_end_offsets = uninitialized_vector<size_t>{15, 19, 25, 16, 20, 21, 14, 14, 15, 17, 20, 24};

  // Sizes in elements
  const auto actual_group_end_offsets = TableRunSource<BasicRun<VariableGroupSizePolicy>>::determine_group_end_offsets(3, 4, value_end_offsets);
  const auto expected_group_end_offsets = uninitialized_vector<size_t>({13, 25, 35, 47});

  EXPECT_EQ(actual_group_end_offsets, expected_group_end_offsets);
}

TEST_F(AggregateHashSortTest,TableRunSourceMaterializeFixedSizeColumn) {
  const auto group_by_column_ids = std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{4}};
  const auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);

  auto run = create_run<BasicRun<VariableGroupSizePolicy>>(setup);

  // clang-format off
  add_group(run, std::optional<int32_t>{}, int64_t{0}, "abcd"s);  // NOLINT
  add_group(run, std::optional<int32_t>{}, int64_t{0}, "iiii"s);  // NOLINT
  add_group(run, std::optional<int32_t>{}, int64_t{0}, ""s);  // NOLINT
  add_group(run, std::optional<int32_t>{}, int64_t{0}, "ccc"s);  // NOLINT
  // clang-format on

  ASSERT_EQ(run.size, 4);
  ASSERT_EQ(run.group_data.size(), run.group_end_offsets[run.size - 1]);

  const auto begin_row_id = RowID{ChunkID{0}, ChunkOffset{1}};
  const auto row_count = size_t{4};

  // Materialize ColumnID{1}, a nullable int32 column
  TableRunSource<BasicRun<VariableGroupSizePolicy>>::materialize_fixed_size_column(run, table, 0, ColumnID{1}, begin_row_id, row_count);

  // clang-format off
  auto expected_group_run_a = create_run<BasicRun<VariableGroupSizePolicy>>(setup);
  add_group(expected_group_run_a, std::optional<int32_t>{13}, int64_t{0}, "abcd"s);  // NOLINT
  add_group(expected_group_run_a, std::optional<int32_t>{},   int64_t{0}, "iiii"s);  // NOLINT
  add_group(expected_group_run_a, std::optional<int32_t>{},   int64_t{0},  ""s);  // NOLINT
  add_group(expected_group_run_a, std::optional<int32_t>{},   int64_t{0}, "ccc"s);  // NOLINT
  // clang-format on

  EXPECT_EQ(run.group_data, expected_group_run_a.group_data);

  // Materialize ColumnID{4}, a non-nullable int64 column
  TableRunSource<BasicRun<VariableGroupSizePolicy>>::materialize_fixed_size_column(run, table, 5, ColumnID{5}, begin_row_id, row_count);

  // clang-format off
  auto expected_group_run_b = create_run<BasicRun<VariableGroupSizePolicy>>(setup);
  add_group(expected_group_run_b, std::optional<int32_t>{13}, int64_t{14}, "abcd"s);  // NOLINT
  add_group(expected_group_run_b, std::optional<int32_t>{},   int64_t{15}, "iiii"s);  // NOLINT
  add_group(expected_group_run_b, std::optional<int32_t>{},   int64_t{16}, ""s);  // NOLINT
  add_group(expected_group_run_b, std::optional<int32_t>{},   int64_t{17}, "ccc"s);  // NOLINT
  // clang-format on

  EXPECT_EQ(run.group_data, expected_group_run_b.group_data);
}

TEST_F(AggregateHashSortTest, TableRunSourceFromTableRange) {
  auto group_by_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5}};
  const auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);

  const auto begin_row_id = RowID{ChunkID{0}, ChunkOffset{0}};
  const auto row_count = 5;
  const auto [run, end_row_id] = TableRunSource<BasicRun<VariableGroupSizePolicy>>::from_table_range(setup, table, begin_row_id, row_count);

  EXPECT_EQ(end_row_id, RowID(ChunkID{2}, ChunkOffset{1}));

  // clang-format off
  auto expected_group_run = create_run<BasicRun<VariableGroupSizePolicy>>(setup);
  add_group(expected_group_run, std::optional<int32_t>{11}, int64_t{12}, std::optional<std::string>{"x"},  "abcd"s,  std::optional<std::string>{"bcdef"});  // NOLINT
  add_group(expected_group_run, std::optional<int32_t>{13}, int64_t{14}, std::optional<std::string>{"y"},  "abcd"s,  std::optional<std::string>{"bcdef"});  // NOLINT
  add_group(expected_group_run, std::optional<int32_t>{},   int64_t{15}, std::optional<std::string>{"xy"}, "iiii"s,  std::optional<std::string>{});  // NOLINT
  add_group(expected_group_run, std::optional<int32_t>{},   int64_t{16}, std::optional<std::string>{},     ""s,      std::optional<std::string>{});  // NOLINT
  add_group(expected_group_run, std::optional<int32_t>{},   int64_t{17}, std::optional<std::string>{"jjj"},"ccc"s,   std::optional<std::string>{"oof"});  // NOLINT
  // clang-format on

  EXPECT_EQ(run.size, 5u);
  EXPECT_EQ(run.group_data, expected_group_run.group_data);
  EXPECT_EQ(run.group_end_offsets, uninitialized_vector<size_t>({13, 26, 38, 48, 60}));

  EXPECT_EQ(run.hashes.size(), 5u);
  EXPECT_NE(run.hashes.at(0), run.hashes.at(1));
  EXPECT_NE(run.hashes.at(0), run.hashes.at(2));
}

TEST_F(AggregateHashSortTest, ProduceInitialAggregates) {
  const auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Double, true}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append({4.5f, 13, 1.0});
  table->append({4.5f, 13, 1.0});
  table->append({NullValue{}, 12, -2.0});
  table->append({1.5f, 14, NullValue{}});
  table->append({1.5f, 14, NullValue{}});

  const auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{
  {std::nullopt, AggregateFunction::CountRows},
  {ColumnID{0}, AggregateFunction::Sum},
  {ColumnID{1}, AggregateFunction::Min},
  {ColumnID{2}, AggregateFunction::Avg},
  };

  const auto setup = AggregateHashSortSetup::create({}, table, aggregate_column_definitions, {ColumnID{0}});

  const auto [run, end_row_id] = TableRunSource<BasicRun<DynamicFixedGroupSizePolicy>>::from_table_range(setup, table, RowID{ChunkID{0}, ChunkOffset{1}}, 3);

  ASSERT_EQ(run.aggregates.size(), 4u);

  const auto* count_rows = dynamic_cast<CountRowsAggregateRun*>(run.aggregates[0].get());
  ASSERT_TRUE(count_rows);
  EXPECT_EQ(count_rows->values, std::vector<int64_t>({1, 1, 1}));

  const auto* sum = dynamic_cast<SumAggregateRun<float>*>(run.aggregates[1].get());
  ASSERT_TRUE(sum);
  EXPECT_EQ(sum->values, std::vector<double>({4.5, 0.0f, 1.5}));
  EXPECT_EQ(sum->null_values, std::vector({false, true, false}));

  const auto* min = dynamic_cast<MinAggregateRun<int32_t>*>(run.aggregates[2].get());
  ASSERT_TRUE(min);
  EXPECT_EQ(min->values, std::vector<int32_t>({13, 12, 14}));
  EXPECT_EQ(min->null_values, std::vector({false, false, false}));

  const auto* avg = dynamic_cast<AvgAggregateRun<double>*>(run.aggregates[3].get());
  ASSERT_TRUE(avg);
  const auto expected_avg_aggregates = std::vector<std::pair<double, size_t>>({{1.0, 1}, {-2.0, 1}, {0.0, 0}});
  EXPECT_EQ(avg->pairs, expected_avg_aggregates);
}

TEST_F(AggregateHashSortTest, RadixFanOut) {
  EXPECT_EQ(RadixFanOut::for_level(0, 2), RadixFanOut(4, 62, 0b11));
  EXPECT_EQ(RadixFanOut::for_level(3, 2), RadixFanOut(4, 56, 0b11));
  EXPECT_EQ(RadixFanOut::for_level(3, 8), RadixFanOut(256, 32, 0b11111111));
  EXPECT_EQ(RadixFanOut::for_level(7, 8), RadixFanOut(256, 0, 0b11111111));
  EXPECT_EQ(RadixFanOut::for_level(8, 7), RadixFanOut(128, 1, 0b1111111));

  EXPECT_ANY_THROW(RadixFanOut::for_level(9, 7));
  EXPECT_ANY_THROW(RadixFanOut::for_level(8, 8));

  EXPECT_EQ(RadixFanOut(8, 1, 0b111).get_partition_for_hash(0b110101), 0b010);
  EXPECT_EQ(RadixFanOut(8, 1, 0b111).get_partition_for_hash(0b111010), 0b101);
}

TEST_F(AggregateHashSortTest, PartitionFixedOnly) {
  using Run = BasicRun<DynamicFixedGroupSizePolicy>;

  const auto group_by_column_ids = std::vector{ColumnID{4}};
  const auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{{ColumnID{1}, AggregateFunction::Sum}};

  const auto setup = AggregateHashSortSetup::create({}, table, aggregate_column_definitions, group_by_column_ids);

  auto run = create_run<Run>(setup);

  // clang-format off
  add_group(run, int32_t{5}); run.hashes[0] = size_t{0b001};
  add_group(run, int32_t{2}); run.hashes[1] = size_t{0b100};
  add_group(run, int32_t{5}); run.hashes[2] = size_t{0b101};
  add_group(run, int32_t{6}); run.hashes[3] = size_t{0b110};
  add_group(run, int32_t{7}); run.hashes[4] = size_t{0b000};
  add_group(run, int32_t{8}); run.hashes[5] = size_t{0b100};
  add_group(run, int32_t{8}); run.hashes[6] = size_t{0b100};
  // clang-format off

  auto* sum_aggregate = dynamic_cast<SumAggregateRun<int32_t>*>(run.aggregates.at(0).get());
  // clang-format off
  sum_aggregate->values =      {0,     1,     2,     3,     0,    5,     6};
  sum_aggregate->null_values = {false, false, false, false, true, false, false};
  // clang-format on

  auto runs = std::vector<BasicRun<DynamicFixedGroupSizePolicy>>{};
  runs.emplace_back(std::move(run));

  auto fan_out = RadixFanOut{4, 1, 3};

  const auto run_source = std::make_shared<PartitionRunSource<Run>>(std::move(runs));
  const auto abstract_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(run_source);

  auto partitions = std::vector<Partition<Run>>{fan_out.partition_count};
  partition<Run>(setup, 6, run_source, fan_out, partitions, 0);
  EXPECT_EQ(run_source->run_idx, 0);
  EXPECT_EQ(run_source->run_offset, 6);
  EXPECT_EQ(run_source->remaining_fetched_group_count, 1);
  EXPECT_EQ(run_source->remaining_fetched_group_data_size, 1);

  ASSERT_EQ(partitions.size(), 4u);

  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 0u);
  ASSERT_EQ(partitions.at(2).runs.size(), 1u);
  ASSERT_EQ(partitions.at(3).runs.size(), 1u);

  ASSERT_EQ(partitions.at(0).runs.at(0).group_data, uninitialized_vector<uint32_t>({5, 7}));
  ASSERT_EQ(partitions.at(0).runs.at(0).hashes, uninitialized_vector<size_t>({0b001, 0b000}));
  ASSERT_EQ(partitions.at(0).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_0 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(0).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_0);
  EXPECT_EQ(aggregate_0->values, std::vector<int64_t>({0, 0}));
  EXPECT_EQ(aggregate_0->null_values, std::vector<bool>({false, true}));

  ASSERT_EQ(partitions.at(2).runs.at(0).group_data, uninitialized_vector<uint32_t>({2, 5, 8}));
  ASSERT_EQ(partitions.at(2).runs.at(0).hashes, uninitialized_vector<size_t>({0b100, 0b101, 0b100}));
  ASSERT_EQ(partitions.at(2).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_2 =
      dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(2).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_2);
  EXPECT_EQ(aggregate_2->values, std::vector<int64_t>({1, 2, 5}));
  EXPECT_EQ(aggregate_2->null_values, std::vector<bool>({false, false, false}));

  ASSERT_EQ(partitions.at(3).runs.at(0).group_data, uninitialized_vector<uint32_t>({6}));
  ASSERT_EQ(partitions.at(3).runs.at(0).hashes, uninitialized_vector<size_t>({0b110}));
  ASSERT_EQ(partitions.at(3).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_3 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(3).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_3);
  EXPECT_EQ(aggregate_3->values, std::vector<int64_t>({3}));
  EXPECT_EQ(aggregate_3->null_values, std::vector<bool>({false}));
}

TEST_F(AggregateHashSortTest, PartitionVariablySized) {
  using Run = BasicRun<VariableGroupSizePolicy>;

  const auto group_by_column_ids = std::vector{ColumnID{0}, ColumnID{1}};
  const auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);

  auto run = create_run<Run>(setup);

  // clang-format off
  add_group(run, std::optional<int32_t>{1}, std::optional<std::string>{"a"});   run.hashes.back() = 0b1;
  add_group(run, std::optional<int32_t>{2}, std::optional<std::string>{});      run.hashes.back() = 0b0;
  add_group(run, std::optional<int32_t>{},  std::optional<std::string>{"bc"});  run.hashes.back() = 0b0;
  add_group(run, std::optional<int32_t>{3}, std::optional<std::string>{"def"}); run.hashes.back() = 0b1;
  // clang-format on

  auto fan_out = RadixFanOut{2, 0, 1};

  auto runs = std::vector<Run>{};
  runs.emplace_back(std::move(run));

  const auto run_source = std::make_shared<PartitionRunSource<Run>>(std::move(runs));
  const auto abstract_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(run_source);

  auto partitions = std::vector<Partition<Run>>{fan_out.partition_count};
  // Partition the entire run
  partition<Run>(setup, 4, run_source, fan_out, partitions, 0);
  EXPECT_EQ(run_source->run_idx, 0);
  EXPECT_EQ(run_source->run_offset, 4);

  ASSERT_EQ(partitions.size(), 2);

  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  auto run_partition_a = create_run<Run>(setup);
  auto run_partition_b = create_run<Run>(setup);

  // clang-format off
  add_group(run_partition_b, std::optional<int32_t>{1}, std::optional<std::string>{"a"});
  add_group(run_partition_a, std::optional<int32_t>{2}, std::optional<std::string>{});
  add_group(run_partition_a, std::optional<int32_t>{},  std::optional<std::string>{"bc"});
  add_group(run_partition_b, std::optional<int32_t>{3}, std::optional<std::string>{"def"});
  // clang-format on

  // clang-format on
  EXPECT_FALSE(partitions.at(0).runs.at(0).is_aggregated);
  EXPECT_EQ(partitions.at(0).runs.at(0).group_data, run_partition_a.group_data);
  EXPECT_FALSE(partitions.at(0).runs.at(0).is_aggregated);
  EXPECT_EQ(partitions.at(1).runs.at(0).group_data, run_partition_b.group_data);
}

TEST_F(AggregateHashSortTest, HashingFixed) {
  using Run = BasicRun<DynamicFixedGroupSizePolicy>;

  const auto group_by_column_ids = std::vector{ColumnID{4}, ColumnID{4}};
  const auto aggregate_column_definitions = std::vector<AggregateColumnDefinition>{{ColumnID{1}, AggregateFunction::Sum}};

  auto setup = AggregateHashSortSetup::create({}, table, aggregate_column_definitions, group_by_column_ids);
  setup.config.hash_table_max_load_factor = 1.0f;
  // Provoke `continue_hashing` to be true
  setup.config.continue_hashing_density_threshold = 1.3f;

  auto run = create_run<Run>(setup);

  // clang-format off
  add_group(run, int32_t{5}, int32_t{3}); run.hashes.back() = 12;
  add_group(run, int32_t{2}, int32_t{2}); run.hashes.back() = 13;
  add_group(run, int32_t{5}, int32_t{3}); run.hashes.back() = 12;
  add_group(run, int32_t{5}, int32_t{4}); run.hashes.back() = 12;
  // clang-format on

  auto* aggregate = dynamic_cast<SumAggregateRun<int32_t>*>(run.aggregates.at(0).get());
  // clang-format off
  aggregate->values =      {5,     6,     7,     0};
  aggregate->null_values = {false, false, false, true};
  // clang-format on

  auto runs = std::vector<Run>{};
  runs.emplace_back(std::move(run));

  auto fan_out = RadixFanOut{2, 0, 1};

  const auto run_source = std::make_shared<PartitionRunSource<Run>>(std::move(runs));
  const auto abstract_run_source =std::static_pointer_cast<AbstractRunSource<Run>>(run_source);
  
  auto partitions = std::vector<Partition<Run>>{fan_out.partition_count};
  
  // Config hashing() so that the entire input is hashed
  const auto [continue_hashing, processed_row_count] =
      hashing<Run>(setup, 3, run_source, fan_out, partitions, 0);
  
  EXPECT_EQ(run_source->run_idx, 0);
  EXPECT_EQ(run_source->run_offset, 4);
  EXPECT_EQ(processed_row_count, 4);
  EXPECT_TRUE(continue_hashing);

  ASSERT_EQ(partitions.size(), 2u);

  ASSERT_EQ(partitions.at(0).size(), 2u);
  ASSERT_EQ(partitions.at(1).size(), 1u);

  ASSERT_EQ(partitions.at(0).runs.at(0).group_data, uninitialized_vector<uint32_t>({5, 3, 5, 4}));
  ASSERT_EQ(partitions.at(0).runs.at(0).hashes, uninitialized_vector<size_t>({12, 12}));
  ASSERT_EQ(partitions.at(0).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_0 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(0).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_0);
  EXPECT_EQ(aggregate_0->values, std::vector<int64_t>({12, 0}));
  EXPECT_EQ(aggregate_0->null_values, std::vector<bool>({false, true}));

  ASSERT_EQ(partitions.at(1).runs.at(0).group_data, uninitialized_vector<uint32_t>({2, 2}));
  ASSERT_EQ(partitions.at(1).runs.at(0).hashes, uninitialized_vector<size_t>({13}));
  ASSERT_EQ(partitions.at(1).runs.at(0).aggregates.size(), 1u);
  const auto* aggregate_1 = dynamic_cast<SumAggregateRun<int32_t>*>(partitions.at(1).runs.at(0).aggregates.at(0).get());
  ASSERT_TRUE(aggregate_1);
  EXPECT_EQ(aggregate_1->values, std::vector<int64_t>({6}));
  EXPECT_EQ(aggregate_1->null_values, std::vector<bool>({false}));
}

TEST_F(AggregateHashSortTest, HashingVariablySized) {
  using Run = BasicRun<VariableGroupSizePolicy>;

  const auto group_by_column_ids = std::vector{ColumnID{0}, ColumnID{1}};

  auto setup = AggregateHashSortSetup::create({}, table, {}, group_by_column_ids);
  setup.config.hash_table_max_load_factor = 1.0f;
  // Provoke `continue_hashing` to be false
  setup.config.continue_hashing_density_threshold = 1.5f;

  auto run = create_run<Run>(setup);

  // clang-format off
  add_group(run, std::optional<int32_t>{1}, std::optional<std::string>{"a"});     run.hashes.back() = 0b0;
  add_group(run, std::optional<int32_t>{2}, std::optional<std::string>{"hell"});  run.hashes.back() = 0b1;
  add_group(run, std::optional<int32_t>{2}, std::optional<std::string>{"hello"}); run.hashes.back() = 0b1;
  add_group(run, std::optional<int32_t>{2}, std::optional<std::string>{"hell"});  run.hashes.back() = 0b1;
  // clang-format on

  auto fan_out = RadixFanOut{2, 0, 1};

  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>();

  auto runs = std::vector<Run>{};
  runs.emplace_back(std::move(run));

  const auto run_source = std::make_shared<PartitionRunSource<Run>>(std::move(runs));
  const auto abstract_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(run_source);
  auto partitions = std::vector<Partition<Run>>{fan_out.partition_count};

  // Configure hashing() so that the entire input is aggregated
  const auto [continue_hashing, processed_row_count] =
      hashing<Run>(setup, 3, run_source, fan_out, partitions, 0);
  EXPECT_EQ(run_source->run_idx, 0);
  EXPECT_EQ(run_source->run_offset, 4);
  EXPECT_EQ(processed_row_count, 4);
  EXPECT_FALSE(continue_hashing);

  ASSERT_EQ(partitions.size(), 2u);
  ASSERT_EQ(partitions.at(0).runs.size(), 1u);
  ASSERT_EQ(partitions.at(1).runs.size(), 1u);

  auto group_run_partition_a = create_run<Run>(setup);
  auto group_run_partition_b = create_run<Run>(setup);

  // clang-format off
  add_group(group_run_partition_a, std::optional<int32_t>{1}, std::optional<std::string>{"a"});
  add_group(group_run_partition_b, std::optional<int32_t>{2}, std::optional<std::string>{"hell"});
  add_group(group_run_partition_b, std::optional<int32_t>{2}, std::optional<std::string>{"hello"});
  // clang-format on

  EXPECT_TRUE(partitions.at(0).runs.at(0).is_aggregated);
  EXPECT_EQ(partitions.at(0).runs.at(0).group_data, group_run_partition_a.group_data);
  EXPECT_TRUE(partitions.at(1).runs.at(0).is_aggregated);
  EXPECT_EQ(partitions.at(1).runs.at(0).group_data, group_run_partition_b.group_data);
}

TEST_F(AggregateHashSortTest, AggregateAdaptive) {
  using Run = BasicRun<StaticFixedGroupSizePolicy<1>>;

  const auto group_by_column_ids = std::vector{ColumnID{4}};

  // Config so that [0,3] are hashed, [4,5] are partitioned and [6,7] are hashed again
  AggregateHashSortConfig config;
  config.hash_table_size = 2;
  config.hash_table_max_load_factor = 0.5f;
  config.max_partitioning_counter = 2;

  const auto setup = AggregateHashSortSetup::create(config, table, {}, group_by_column_ids);

  auto run = create_run<Run>(setup);

  // clang-format off
  add_group(run, int32_t{5}); run.hashes.back() = size_t{0b1};
  add_group(run, int32_t{3}); run.hashes.back() = size_t{0b1};
  add_group(run, int32_t{5}); run.hashes.back() = size_t{0b1};
  add_group(run, int32_t{2}); run.hashes.back() = size_t{0b0};
  add_group(run, int32_t{3}); run.hashes.back() = size_t{0b1};
  add_group(run, int32_t{5}); run.hashes.back() = size_t{0b1};
  add_group(run, int32_t{2}); run.hashes.back() = size_t{0b0};
  add_group(run, int32_t{2}); run.hashes.back() = size_t{0b0};
  // clang-format off

  auto runs = std::vector<Run>{};
  runs.emplace_back(std::move(run));

  auto fan_out = RadixFanOut{2, 0, 1};

  const auto run_source = std::make_shared<PartitionRunSource<Run>>(std::move(runs));
  const auto abstract_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(run_source);
  const auto partitions = adaptive_hashing_and_partition<Run>(setup, abstract_run_source, fan_out, 0);

  ASSERT_EQ(partitions.size(), 2u);

  ASSERT_EQ(partitions.at(0).size(), 2u);
  ASSERT_EQ(partitions.at(1).size(), 4u);

  ASSERT_EQ(partitions.at(0).runs.size(), 2u);
  ASSERT_EQ(partitions.at(1).runs.size(), 2u);

  EXPECT_EQ(partitions.at(0).runs.at(0).is_aggregated, false);
  EXPECT_EQ(partitions.at(0).runs.at(1).is_aggregated, true);
  EXPECT_EQ(partitions.at(1).runs.at(0).is_aggregated, false);
  EXPECT_EQ(partitions.at(1).runs.at(1).is_aggregated, false);

  EXPECT_EQ(partitions.at(0).runs.at(0).group_data, uninitialized_vector<uint32_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(0).hashes, uninitialized_vector<size_t>({0b0}));
  EXPECT_EQ(partitions.at(0).runs.at(1).group_data, uninitialized_vector<uint32_t>({2}));
  EXPECT_EQ(partitions.at(0).runs.at(1).hashes, uninitialized_vector<size_t>({0b0}));

  EXPECT_EQ(partitions.at(1).runs.at(0).group_data, uninitialized_vector<uint32_t>({5, 3}));
  EXPECT_EQ(partitions.at(1).runs.at(0).hashes, uninitialized_vector<size_t>({0b1, 0b1}));
  EXPECT_EQ(partitions.at(1).runs.at(1).group_data, uninitialized_vector<uint32_t>({3, 5}));
  EXPECT_EQ(partitions.at(1).runs.at(1).hashes, uninitialized_vector<size_t>({0b1, 0b1}));
}

}  // namespace opossum
