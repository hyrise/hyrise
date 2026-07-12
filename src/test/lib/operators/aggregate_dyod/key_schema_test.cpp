#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "null_value.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

namespace {

struct PackInput {
  std::shared_ptr<Table> table;
  std::vector<std::shared_ptr<AbstractSegment>> segment_owners;
  std::vector<const AbstractSegment*> segments;
  std::vector<ColumnID> column_ids;
};

PackInput make_pack_input(const TableColumnDefinitions& definitions,
                          const std::vector<std::vector<AllTypeVariant>>& rows) {
  auto input = PackInput{};
  input.table = std::make_shared<Table>(definitions, TableType::Data);
  for (const auto& row : rows) {
    input.table->append(row);
  }
  const auto chunk = input.table->get_chunk(ChunkID{0});
  for (auto column_index = size_t{0}; column_index < definitions.size(); ++column_index) {
    const auto column_id = ColumnID{static_cast<ColumnID::base_type>(column_index)};
    input.column_ids.emplace_back(column_id);
    input.segment_owners.emplace_back(chunk->get_segment(column_id));
    input.segments.emplace_back(input.segment_owners.back().get());
  }
  return input;
}

// Prefilled with garbage so pack() has to overwrite every byte.
template <typename KeySchema>
std::vector<std::byte> pack_key(const KeySchema& schema, const PackInput& input, uint32_t row,
                                StringSpillBuffer& spill_buffer) {
  auto key = std::vector<std::byte>(schema.packed_width(), std::byte{0xAA});
  schema.pack(std::span<const AbstractSegment* const>{input.segments}, ChunkOffset{row}, key.data(), spill_buffer);
  return key;
}

template <typename KeySchema>
std::vector<std::vector<std::byte>> pack_all_keys(const KeySchema& schema, const PackInput& input,
                                                  StringSpillBuffer& spill_buffer) {
  auto keys = std::vector<std::vector<std::byte>>{};
  const auto row_count = input.table->row_count();
  for (auto row = uint32_t{0}; row < row_count; ++row) {
    keys.emplace_back(pack_key(schema, input, row, spill_buffer));
  }
  return keys;
}

template <typename KeySchema>
void expect_keys_equal(const KeySchema& schema, const std::vector<std::byte>& a, const std::vector<std::byte>& b) {
  EXPECT_TRUE(schema.equals(a.data(), b.data()));
  EXPECT_TRUE(schema.equals(b.data(), a.data()));
  EXPECT_EQ(schema.hash(a.data()), schema.hash(b.data()));
}

template <typename KeySchema>
void expect_keys_not_equal(const KeySchema& schema, const std::vector<std::byte>& a, const std::vector<std::byte>& b) {
  EXPECT_FALSE(schema.equals(a.data(), b.data()));
  EXPECT_FALSE(schema.equals(b.data(), a.data()));
}

template <typename KeySchema>
void expect_unpack_round_trip(const KeySchema& schema, const TableColumnDefinitions& definitions,
                              const std::vector<std::vector<std::byte>>& keys,
                              const std::vector<std::vector<AllTypeVariant>>& expected_rows) {
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  for (auto row = size_t{0}; row < keys.size(); ++row) {
    schema.unpack(keys[row].data(), output, row);
  }
  output.seal_all();

  for (auto column_index = size_t{0}; column_index < definitions.size(); ++column_index) {
    const auto segments = output.column(column_index).take_segments();
    ASSERT_EQ(segments.size(), 1u);
    ASSERT_EQ(segments[0]->size(), keys.size());
    for (auto row = size_t{0}; row < keys.size(); ++row) {
      const auto actual = (*segments[0])[ChunkOffset{static_cast<uint32_t>(row)}];
      const auto& expected = expected_rows[row][column_index];
      if (variant_is_null(expected)) {
        EXPECT_TRUE(variant_is_null(actual)) << "row " << row << ", column " << column_index << " should be NULL";
      } else {
        EXPECT_EQ(actual, expected) << "row " << row << ", column " << column_index;
      }
    }
  }
}

}  // namespace

class AggregateDYODKeySchemaTest : public BaseTest {};

TEST_F(AggregateDYODKeySchemaTest, SpillBufferCopiesContentOutOfTheSource) {
  auto buffer = StringSpillBuffer{};

  auto source = std::string{"the quick brown fox"};
  const auto* interned = buffer.append(reinterpret_cast<const std::byte*>(source.data()), source.size());

  const auto original = source;
  std::fill(source.begin(), source.end(), '?');

  ASSERT_NE(interned, nullptr);
  EXPECT_EQ(std::memcmp(interned, original.data(), original.size()), 0);
}

TEST_F(AggregateDYODKeySchemaTest, SpillBufferPointersSurviveLaterAppends) {
  auto buffer = StringSpillBuffer{};

  // Enough content that the buffer must grow beyond any plausible initial block.
  constexpr auto APPEND_COUNT = size_t{1'000};
  constexpr auto CONTENT_LENGTH = size_t{257};

  auto interned_pointers = std::vector<const std::byte*>{};
  for (auto index = size_t{0}; index < APPEND_COUNT; ++index) {
    auto content = std::string(CONTENT_LENGTH, static_cast<char>('a' + (index % 26)));
    interned_pointers.emplace_back(buffer.append(reinterpret_cast<const std::byte*>(content.data()), content.size()));
  }

  for (auto index = size_t{0}; index < APPEND_COUNT; ++index) {
    const auto expected = std::string(CONTENT_LENGTH, static_cast<char>('a' + (index % 26)));
    EXPECT_EQ(std::memcmp(interned_pointers[index], expected.data(), expected.size()), 0) << "append #" << index;
  }
}

TEST_F(AggregateDYODKeySchemaTest, SpillBufferIsReusableAfterClear) {
  auto buffer = StringSpillBuffer{};

  const auto first = std::string{"first batch"};
  buffer.append(reinterpret_cast<const std::byte*>(first.data()), first.size());
  buffer.clear();

  const auto second = std::string{"second batch"};
  const auto* interned = buffer.append(reinterpret_cast<const std::byte*>(second.data()), second.size());

  ASSERT_NE(interned, nullptr);
  EXPECT_EQ(std::memcmp(interned, second.data(), second.size()), 0);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvingWithoutGroupByColumnsThrows) {
  const auto input = make_pack_input({{"a", DataType::Int, false}}, {{1}});
  EXPECT_THROW(resolve_key_schema({}, *input.table, [](const auto& /*schema*/) {}), std::logic_error);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesSingleIntToFourByteShortSchema) {
  const auto input = make_pack_input({{"a", DataType::Int, false}}, {{1}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericShortKeySchema<4>>));
    EXPECT_EQ(schema.packed_width(), 4u);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesIntPlusLongToTwelveByteShortSchema) {
  const auto input = make_pack_input({{"a", DataType::Int, false}, {"b", DataType::Long, false}}, {{1, int64_t{2}}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericShortKeySchema<12>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesTwoLongsToSixteenByteShortSchema) {
  const auto input =
      make_pack_input({{"a", DataType::Long, false}, {"b", DataType::Long, false}}, {{int64_t{1}, int64_t{2}}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericShortKeySchema<16>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, NullableColumnWidensTheKeyByTheNullBitmap) {
  const auto input = make_pack_input({{"a", DataType::Int, true}}, {{1}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericShortKeySchema<8>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesWideNumericTupleToArbitrarySchema) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false},
                                                  {"b", DataType::Int, false},
                                                  {"c", DataType::Int, false},
                                                  {"d", DataType::Int, false},
                                                  {"e", DataType::Int, false}};
  const auto input = make_pack_input(definitions, {{1, 2, 3, 4, 5}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericArbitraryKeySchema>));
    EXPECT_EQ(schema.packed_width(), 20u);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesAllStringsToStringOnlySchema) {
  const auto input = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"x"}}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_EQ(SchemaType::COMPOSITION, KeyComposition::StringOnly);
    EXPECT_TRUE(SchemaType::HAS_STRINGS);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesStringPlusNumericToMixedSchema) {
  const auto input =
      make_pack_input({{"a", DataType::Int, false}, {"b", DataType::String, false}}, {{1, pmr_string{"x"}}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_EQ(SchemaType::COMPOSITION, KeyComposition::Mixed);
    EXPECT_TRUE(SchemaType::HAS_STRINGS);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, StringSchemaPackedWidthIsFixedPartPlusSpillPointer) {
  const auto input = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"x"}}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    if constexpr (SchemaType::HAS_STRINGS) {
      EXPECT_EQ(schema.packed_width(), schema.fixed_part_width() + 8u);
    } else {
      FAIL() << "Expected a string-involving schema.";
    }
  });
}

TEST_F(AggregateDYODKeySchemaTest, PacksEqualIntsToEqualKeys) {
  const auto input = make_pack_input({{"a", DataType::Int, false}}, {{42}, {42}, {7}});
  const auto schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  EXPECT_EQ(schema.packed_width(), 4u);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  expect_keys_not_equal(schema, keys[0], keys[2]);
}

TEST_F(AggregateDYODKeySchemaTest, DistinguishesFullIntRange) {
  const auto min = std::numeric_limits<int32_t>::min();
  const auto max = std::numeric_limits<int32_t>::max();
  const auto input = make_pack_input({{"a", DataType::Int, false}}, {{min}, {-1}, {0}, {1}, {max}});
  const auto schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  for (auto first = size_t{0}; first < keys.size(); ++first) {
    for (auto second = first + 1; second < keys.size(); ++second) {
      expect_keys_not_equal(schema, keys[first], keys[second]);
    }
  }
}

TEST_F(AggregateDYODKeySchemaTest, TreatsNullAsDistinctFromEveryValue) {
  const auto min = std::numeric_limits<int32_t>::min();
  const auto max = std::numeric_limits<int32_t>::max();
  const auto input = make_pack_input({{"a", DataType::Int, true}}, {{NullValue{}}, {NullValue{}}, {0}, {min}, {max}});
  const auto schema = NumericShortKeySchema<8>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  for (auto value_row = size_t{2}; value_row < keys.size(); ++value_row) {
    expect_keys_not_equal(schema, keys[0], keys[value_row]);
  }
}

TEST_F(AggregateDYODKeySchemaTest, CanonicalizesFloatZeroesAndNaNs) {
  const auto quiet_nan = std::numeric_limits<float>::quiet_NaN();
  const auto payload_nan = std::nanf("2");
  const auto input =
      make_pack_input({{"a", DataType::Float, false}}, {{-0.0f}, {0.0f}, {quiet_nan}, {payload_nan}, {1.0f}});
  const auto schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  expect_keys_equal(schema, keys[2], keys[3]);
  expect_keys_not_equal(schema, keys[2], keys[4]);
  expect_keys_not_equal(schema, keys[0], keys[4]);
}

TEST_F(AggregateDYODKeySchemaTest, MultiColumnKeysCompareLaneWise) {
  const auto big = int64_t{1} << 40;
  const auto input = make_pack_input({{"a", DataType::Int, false}, {"b", DataType::Long, false}},
                                     {{1, big}, {1, big}, {2, big}, {1, big + 1}});
  const auto schema = NumericShortKeySchema<12>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  expect_keys_not_equal(schema, keys[0], keys[2]);
  expect_keys_not_equal(schema, keys[0], keys[3]);
}

TEST_F(AggregateDYODKeySchemaTest, ArbitraryWidthSchemaComparesTheFullKey) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false},
                                                  {"b", DataType::Int, false},
                                                  {"c", DataType::Int, false},
                                                  {"d", DataType::Int, false},
                                                  {"e", DataType::Int, false}};
  const auto input = make_pack_input(definitions, {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 6}});
  const auto schema = NumericArbitraryKeySchema::build(input.column_ids, *input.table);
  EXPECT_EQ(schema.packed_width(), 20u);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  expect_keys_not_equal(schema, keys[0], keys[2]);
}

TEST_F(AggregateDYODKeySchemaTest, NumericKeysRoundTripThroughUnpack) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::Long, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {NullValue{}, int64_t{100}},
      {42, std::numeric_limits<int64_t>::min()},
      {std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max()},
      {-1, int64_t{0}},
  };
  const auto input = make_pack_input(definitions, rows);
  const auto schema = NumericShortKeySchema<16>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_unpack_round_trip(schema, definitions, keys, rows);
}

TEST_F(AggregateDYODKeySchemaTest, FloatAndDoubleKeysRoundTripThroughUnpack) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Float, false}, {"b", DataType::Double, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {1.5f, 2.25},
      {-3.75f, std::numeric_limits<double>::lowest()},
      {std::numeric_limits<float>::max(), -0.0},
  };
  const auto input = make_pack_input(definitions, rows);
  const auto schema = NumericShortKeySchema<12>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_unpack_round_trip(schema, definitions, keys, rows);
}

TEST_F(AggregateDYODKeySchemaTest, ArbitraryWidthKeysRoundTripThroughUnpack) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                  {"b", DataType::Int, false},
                                                  {"c", DataType::Int, false},
                                                  {"d", DataType::Int, false},
                                                  {"e", DataType::Int, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {NullValue{}, 2, 3, 4, 5},
      {1, 2, 3, 4, 5},
      {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max(), 0, -1, 1},
  };
  const auto input = make_pack_input(definitions, rows);
  const auto schema = NumericArbitraryKeySchema::build(input.column_ids, *input.table);
  EXPECT_EQ(schema.packed_width(), 24u);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_unpack_round_trip(schema, definitions, keys, rows);
}

TEST_F(AggregateDYODKeySchemaTest, PacksEqualInlineStringsToEqualKeys) {
  const auto input = make_pack_input(
      {{"a", DataType::String, false}},
      {{pmr_string{"abc"}}, {pmr_string{"abc"}}, {pmr_string{"abd"}}, {pmr_string{""}}, {pmr_string{""}}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[1]);
    expect_keys_equal(schema, keys[3], keys[4]);
    expect_keys_not_equal(schema, keys[0], keys[2]);
    expect_keys_not_equal(schema, keys[0], keys[3]);
  });
}

TEST_F(AggregateDYODKeySchemaTest, NullStringIsDistinctFromEmptyString) {
  const auto input = make_pack_input({{"a", DataType::String, true}}, {{NullValue{}}, {NullValue{}}, {pmr_string{""}}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[1]);
    expect_keys_not_equal(schema, keys[0], keys[2]);
  });
}

TEST_F(AggregateDYODKeySchemaTest, EmbeddedNulBytesDoNotTruncateStringKeys) {
  const auto with_nul_b = pmr_string{"a\0b", 3};
  const auto with_nul_c = pmr_string{"a\0c", 3};
  const auto input =
      make_pack_input({{"a", DataType::String, false}}, {{with_nul_b}, {pmr_string{"a"}}, {with_nul_c}, {with_nul_b}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[3]);
    expect_keys_not_equal(schema, keys[0], keys[1]);
    expect_keys_not_equal(schema, keys[0], keys[2]);
    expect_keys_not_equal(schema, keys[1], keys[2]);
  });
}

TEST_F(AggregateDYODKeySchemaTest, AdjacentStringColumnsDoNotBleedIntoEachOther) {
  // Both rows concatenate to "abc".
  const auto input = make_pack_input(
      {{"a", DataType::String, false}, {"b", DataType::String, false}},
      {{pmr_string{"ab"}, pmr_string{"c"}}, {pmr_string{"a"}, pmr_string{"bc"}}, {pmr_string{"ab"}, pmr_string{"c"}}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[2]);
    expect_keys_not_equal(schema, keys[0], keys[1]);
  });
}

TEST_F(AggregateDYODKeySchemaTest, SpilledStringsCompareByContentAcrossSpillBuffers) {
  const auto long_a = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'x');
  auto long_c = long_a;
  long_c.back() = 'y';
  const auto input =
      make_pack_input({{"a", DataType::String, false}}, {{long_a}, {long_a}, {long_c}, {pmr_string{"x"}}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill_a = StringSpillBuffer{};
    auto spill_b = StringSpillBuffer{};
    const auto key_a = pack_key(schema, input, 0, spill_a);
    const auto key_b = pack_key(schema, input, 1, spill_b);
    const auto key_c = pack_key(schema, input, 2, spill_b);
    const auto key_short = pack_key(schema, input, 3, spill_a);

    expect_keys_equal(schema, key_a, key_b);
    expect_keys_not_equal(schema, key_a, key_c);
    expect_keys_not_equal(schema, key_a, key_short);
  });
}

TEST_F(AggregateDYODKeySchemaTest, StringsAtTheBlobCapacityBoundaryStayComparable) {
  const auto exact_fit = pmr_string(STRING_BLOB_BYTES_PER_COLUMN, 'e');
  const auto one_past = pmr_string(STRING_BLOB_BYTES_PER_COLUMN + 1, 'p');
  auto exact_fit_other = exact_fit;
  exact_fit_other.back() = 'f';
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}};
  const auto rows =
      std::vector<std::vector<AllTypeVariant>>{{exact_fit}, {exact_fit}, {one_past}, {one_past}, {exact_fit_other}};
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill_a = StringSpillBuffer{};
    auto spill_b = StringSpillBuffer{};
    const auto keys_a = pack_all_keys(schema, input, spill_a);
    const auto key_one_past_b = pack_key(schema, input, 3, spill_b);

    expect_keys_equal(schema, keys_a[0], keys_a[1]);
    expect_keys_equal(schema, keys_a[2], key_one_past_b);
    expect_keys_not_equal(schema, keys_a[0], keys_a[2]);
    expect_keys_not_equal(schema, keys_a[0], keys_a[4]);

    expect_unpack_round_trip(schema, definitions, keys_a, rows);
  });
}

TEST_F(AggregateDYODKeySchemaTest, NullStringBesideSpilledContentIsPreserved) {
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 's');
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, true}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {NullValue{}, long_string}, {NullValue{}, long_string}, {pmr_string{"x"}, long_string}};
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill_a = StringSpillBuffer{};
    auto spill_b = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill_a);
    const auto key_null_b = pack_key(schema, input, 1, spill_b);

    expect_keys_equal(schema, keys[0], key_null_b);
    expect_keys_not_equal(schema, keys[0], keys[2]);

    expect_unpack_round_trip(schema, definitions, keys, rows);
  });
}

TEST_F(AggregateDYODKeySchemaTest, MixedSchemaCarriesFloatAndDoubleLanes) {
  const auto definitions = TableColumnDefinitions{
      {"a", DataType::Float, false}, {"b", DataType::Double, false}, {"c", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {1.5f, -0.0, pmr_string{"a"}}, {1.5f, 0.0, pmr_string{"a"}}, {2.5f, 0.0, pmr_string{"a"}}};
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[1]);
    expect_keys_not_equal(schema, keys[0], keys[2]);

    expect_unpack_round_trip(schema, definitions, keys, rows);
  });
}

TEST_F(AggregateDYODKeySchemaTest, MixedKeysCompareNumericAndStringParts) {
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'z');
  const auto input =
      make_pack_input({{"a", DataType::Int, false}, {"b", DataType::String, true}}, {{1, pmr_string{"short"}},
                                                                                     {1, pmr_string{"short"}},
                                                                                     {2, pmr_string{"short"}},
                                                                                     {1, pmr_string{"other"}},
                                                                                     {1, NullValue{}},
                                                                                     {7, long_string}});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);

    expect_keys_equal(schema, keys[0], keys[1]);
    expect_keys_not_equal(schema, keys[0], keys[2]);
    expect_keys_not_equal(schema, keys[0], keys[3]);
    expect_keys_not_equal(schema, keys[0], keys[4]);
    expect_keys_not_equal(schema, keys[0], keys[5]);
  });
}

TEST_F(AggregateDYODKeySchemaTest, StringKeysRoundTripThroughUnpack) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, true}};
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'q');
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {pmr_string{""}}, {pmr_string{"hello"}}, {long_string}, {NullValue{}}, {pmr_string{"a\0b", 3}},
  };
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);
    expect_unpack_round_trip(schema, definitions, keys, rows);
  });
}

TEST_F(AggregateDYODKeySchemaTest, MixedKeysRoundTripThroughUnpack) {
  const auto definitions =
      TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, false}, {"c", DataType::Long, false}};
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'm');
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {1, pmr_string{"alpha"}, int64_t{-5}},
      {NullValue{}, pmr_string{""}, std::numeric_limits<int64_t>::max()},
      {std::numeric_limits<int32_t>::min(), long_string, int64_t{0}},
  };
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    auto spill = StringSpillBuffer{};
    const auto keys = pack_all_keys(schema, input, spill);
    expect_unpack_round_trip(schema, definitions, keys, rows);
  });
}

}  // namespace hyrise
