#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "null_value.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

namespace {

struct PackInput {
  std::shared_ptr<const Table> table;
  std::vector<std::shared_ptr<AbstractSegment>> segment_owners;
  std::vector<const AbstractSegment*> segments;
  std::vector<ColumnID> column_ids;
};

void gather_segments(PackInput& input) {
  input.segment_owners.clear();
  input.segments.clear();
  const auto chunk = input.table->get_chunk(ChunkID{0});
  for (const auto column_id : input.column_ids) {
    input.segment_owners.emplace_back(chunk->get_segment(column_id));
    input.segments.emplace_back(input.segment_owners.back().get());
  }
}

PackInput make_pack_input(const TableColumnDefinitions& definitions,
                          const std::vector<std::vector<AllTypeVariant>>& rows,
                          const std::optional<ChunkOffset> target_chunk_size = std::nullopt,
                          const std::optional<SegmentEncodingSpec> encoding = std::nullopt) {
  auto input = PackInput{};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, target_chunk_size);
  for (const auto& row : rows) {
    table->append(row);
  }
  if (encoding) {
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, *encoding);
  }
  input.table = table;
  for (auto column_index = size_t{0}; column_index < definitions.size(); ++column_index) {
    input.column_ids.emplace_back(ColumnID{static_cast<ColumnID::base_type>(column_index)});
  }
  gather_segments(input);
  return input;
}

PackInput to_reference_input(const PackInput& input) {
  auto reference = PackInput{};
  reference.table = to_simple_reference_table(input.table);
  reference.column_ids = input.column_ids;
  gather_segments(reference);
  return reference;
}

PackInput to_single_chunk_reference_input(const PackInput& input) {
  auto pos_list = std::make_shared<RowIDPosList>();
  const auto row_count = input.table->get_chunk(ChunkID{0})->size();
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
    pos_list->emplace_back(RowID{ChunkID{0}, chunk_offset});
  }
  pos_list->guarantee_single_chunk();

  auto definitions = TableColumnDefinitions{};
  auto segments = Segments{};
  for (const auto column_id : input.column_ids) {
    definitions.emplace_back(input.table->column_name(column_id), input.table->column_data_type(column_id),
                             input.table->column_is_nullable(column_id));
    segments.emplace_back(std::make_shared<ReferenceSegment>(input.table, column_id, pos_list));
  }
  const auto table = std::make_shared<Table>(definitions, TableType::References);
  table->append_chunk(segments);

  auto reference = PackInput{};
  reference.table = table;
  reference.column_ids = input.column_ids;
  gather_segments(reference);
  return reference;
}

// Prefilled with garbage so pack() has to overwrite every byte.
template <typename KeySchema>
std::vector<std::byte> pack_key(const KeySchema& schema, const PackInput& input, uint32_t row,
                                StringSpillBuffer& spill_buffer) {
  auto scratch = KeyDecodeScratch{};
  schema.decode(std::span<const AbstractSegment* const>{input.segments}, scratch);
  auto key = std::vector<std::byte>(schema.packed_width(), std::byte{0xAA});
  schema.pack(scratch, ChunkOffset{row}, key.data(), spill_buffer);
  return key;
}

template <typename KeySchema>
std::vector<std::vector<std::byte>> pack_all_keys(const KeySchema& schema, const PackInput& input,
                                                  StringSpillBuffer& spill_buffer) {
  auto scratch = KeyDecodeScratch{};
  schema.decode(std::span<const AbstractSegment* const>{input.segments}, scratch);
  auto keys = std::vector<std::vector<std::byte>>{};
  const auto row_count = input.table->row_count();
  for (auto row = uint32_t{0}; row < row_count; ++row) {
    auto key = std::vector<std::byte>(schema.packed_width(), std::byte{0xAA});
    schema.pack(scratch, ChunkOffset{row}, key.data(), spill_buffer);
    keys.emplace_back(std::move(key));
  }
  return keys;
}

template <typename KeySchema>
std::vector<std::vector<std::byte>> pack_window_keys(const KeySchema& schema, const PackInput& input,
                                                     const size_t row_begin, const size_t row_end,
                                                     StringSpillBuffer& spill_buffer) {
  auto scratch = KeyDecodeScratch{};
  schema.decode(std::span<const AbstractSegment* const>{input.segments}, row_begin, row_end, scratch);
  auto keys = std::vector<std::vector<std::byte>>{};
  const auto window_rows = row_end - row_begin;
  for (auto row = uint32_t{0}; row < window_rows; ++row) {
    auto key = std::vector<std::byte>(schema.packed_width(), std::byte{0xAA});
    schema.pack(scratch, ChunkOffset{row}, key.data(), spill_buffer);
    keys.emplace_back(std::move(key));
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
void expect_window_matches_full_decode(const KeySchema& schema, const PackInput& input, const size_t row_begin,
                                       const size_t row_end) {
  auto full_spill = StringSpillBuffer{};
  auto window_spill = StringSpillBuffer{};
  const auto full = pack_all_keys(schema, input, full_spill);
  const auto window = pack_window_keys(schema, input, row_begin, row_end, window_spill);

  ASSERT_EQ(window.size(), row_end - row_begin);
  for (auto row = size_t{0}; row < window.size(); ++row) {
    expect_keys_equal(schema, full[row_begin + row], window[row]);
  }
}

template <typename KeySchema>
void expect_unpack_round_trip(const KeySchema& schema, const TableColumnDefinitions& definitions,
                              const std::vector<std::vector<std::byte>>& keys,
                              const std::vector<std::vector<AllTypeVariant>>& expected_rows) {
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  for (auto row = size_t{0}; row < keys.size(); ++row) {
    schema.unpack(keys[row].data(), output);
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

TEST_F(AggregateDYODKeySchemaTest, SpillBufferFreesBlocksOnRelease) {
  const auto empty = StringSpillBuffer{}.memory_usage();
  auto buffer = StringSpillBuffer{};

  const auto content = std::string{"spilled key content"};
  buffer.append(reinterpret_cast<const std::byte*>(content.data()), content.size());
  EXPECT_GT(buffer.memory_usage(), empty);

  buffer.release();

  EXPECT_EQ(buffer.memory_usage(), empty);
}

TEST_F(AggregateDYODKeySchemaTest, SpillBufferIsReusableAfterRelease) {
  auto buffer = StringSpillBuffer{};

  const auto first = std::string{"first batch"};
  buffer.append(reinterpret_cast<const std::byte*>(first.data()), first.size());
  buffer.release();

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

TEST_F(AggregateDYODKeySchemaTest, ResolvesFiveIntsToTwentyByteShortSchema) {
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
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericShortKeySchema<20>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesWideNumericTupleToArbitrarySchema) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Long, false},
                                                  {"b", DataType::Long, false},
                                                  {"c", DataType::Long, false},
                                                  {"d", DataType::Int, false}};
  const auto input = make_pack_input(definitions, {{int64_t{1}, int64_t{2}, int64_t{3}, 4}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, NumericArbitraryKeySchema>));
    EXPECT_EQ(schema.packed_width(), 28u);
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

TEST_F(AggregateDYODKeySchemaTest, ResolvesDictionaryBoundedStringsToNarrowLengthFields) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"b", DataType::String, false}};
  const auto rows =
      std::vector<std::vector<AllTypeVariant>>{{pmr_string{"A"}, pmr_string{"F"}}, {pmr_string{"N"}, pmr_string{"O"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, StringOnlyKeySchema<1>>));
    EXPECT_EQ(schema.packed_width(), 12u);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, ResolvesDictionaryBoundedMixedKeysToNarrowLengthFields) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"A"}}, {2, pmr_string{"N"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, MixedKeySchema<1>>));
    EXPECT_EQ(schema.packed_width(), 16u);
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, UnencodedStringColumnsKeepTheDefaultLengthFields) {
  const auto input = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"A"}}, {pmr_string{"N"}}});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, StringOnlyKeySchema<4>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, PartlyEncodedStringColumnsKeepTheDefaultLengthFields) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, ChunkOffset{2});
  for (const auto* value : {"A", "F", "N", "O"}) {
    table->append({pmr_string{value}});
  }
  table->last_chunk()->set_immutable();
  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, SegmentEncodingSpec{EncodingType::Dictionary});

  auto resolved = false;
  resolve_key_schema({ColumnID{0}}, *table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, StringOnlyKeySchema<4>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, LongDictionaryEntriesKeepTheDefaultLengthFields) {
  const auto long_value = pmr_string(256, 'v');
  const auto input = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"A"}}, {long_value}}, std::nullopt,
                                     SegmentEncodingSpec{EncodingType::Dictionary});
  auto resolved = false;
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    using SchemaType = std::decay_t<decltype(schema)>;
    resolved = true;
    EXPECT_TRUE((std::is_same_v<SchemaType, StringOnlyKeySchema<4>>));
  });
  EXPECT_TRUE(resolved);
}

TEST_F(AggregateDYODKeySchemaTest, StringKeyBudgetSumsTheDictionaryMaxima) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{pmr_string{"A"}, pmr_string{"ship"}},
                                                             {pmr_string{"N"}, pmr_string{"air"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});

  const auto budget = choose_string_key_budget(input.column_ids, *input.table, DICTIONARY_BOUND_SCAN_LIMIT);

  EXPECT_EQ(budget.length_field_width, 1u);
  ASSERT_TRUE(budget.blob_bytes.has_value());
  EXPECT_EQ(*budget.blob_bytes, 5u);
}

TEST_F(AggregateDYODKeySchemaTest, StringKeyBudgetCapsTheBlobAtTheDefaultCapacity) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{pmr_string{"A"}, pmr_string(40, 'x')},
                                                             {pmr_string{"N"}, pmr_string{"air"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});

  const auto budget = choose_string_key_budget(input.column_ids, *input.table, DICTIONARY_BOUND_SCAN_LIMIT);

  EXPECT_EQ(budget.length_field_width, 1u);
  ASSERT_TRUE(budget.blob_bytes.has_value());
  EXPECT_EQ(*budget.blob_bytes, 2 * STRING_BLOB_BYTES_PER_COLUMN);
}

TEST_F(AggregateDYODKeySchemaTest, StringKeyBudgetGivesUpOnOversizedDictionaries) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {pmr_string{"A"}}, {pmr_string{"F"}}, {pmr_string{"N"}}, {pmr_string{"O"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});

  EXPECT_EQ(choose_string_key_budget(input.column_ids, *input.table, 4).length_field_width, 1u);

  const auto exceeded = choose_string_key_budget(input.column_ids, *input.table, 3);
  EXPECT_EQ(exceeded.length_field_width, 4u);
  EXPECT_FALSE(exceeded.blob_bytes.has_value());
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

TEST_F(AggregateDYODKeySchemaTest, HashSpreadsStructuredKeysAcrossTheLowBits) {
  auto rows = std::vector<std::vector<AllTypeVariant>>{};
  for (auto k = int64_t{0}; k < 1024; ++k) {
    rows.push_back({k * 1000003});
  }
  const auto input = make_pack_input({{"a", DataType::Long, false}}, rows);

  const auto schema = NumericShortKeySchema<8>::build(input.column_ids, *input.table);
  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  auto buckets = std::unordered_set<uint64_t>{};
  for (const auto& key : keys) {
    buckets.insert(schema.hash(key.data()) & 4095);
  }
  EXPECT_GT(buckets.size(), 850u);
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

TEST_F(AggregateDYODKeySchemaTest, NarrowStringKeysPackLikeTheDefaultLayoutAcrossEncodings) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"abc"}},
                                                             {NullValue{}, pmr_string{"abc"}},
                                                             {1, NullValue{}},
                                                             {2, pmr_string{"d"}},
                                                             {1, pmr_string{"abc"}}};
  const auto plain = make_pack_input(definitions, rows);
  const auto encoded = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  const auto schema = MixedKeySchema<1>::build(encoded.column_ids, *encoded.table, 3);

  auto spill_plain = StringSpillBuffer{};
  auto spill_encoded = StringSpillBuffer{};
  const auto expected = pack_all_keys(schema, plain, spill_plain);
  const auto actual = pack_all_keys(schema, encoded, spill_encoded);
  for (auto row = size_t{0}; row < expected.size(); ++row) {
    expect_keys_equal(schema, expected[row], actual[row]);
  }
  expect_unpack_round_trip(schema, definitions, actual, rows);
}

TEST_F(AggregateDYODKeySchemaTest, NarrowStringKeysDistinguishNullFromEmptyStrings) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, true}};
  const auto rows =
      std::vector<std::vector<AllTypeVariant>>{{NullValue{}}, {NullValue{}}, {pmr_string{""}}, {pmr_string{"x"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  const auto schema = StringOnlyKeySchema<1>::build(input.column_ids, *input.table, 1);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[1]);
  expect_keys_not_equal(schema, keys[0], keys[2]);
  expect_keys_not_equal(schema, keys[2], keys[3]);
  expect_unpack_round_trip(schema, definitions, keys, rows);
}

TEST_F(AggregateDYODKeySchemaTest, AdjacentNarrowStringColumnsDoNotBleedIntoEachOther) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {pmr_string{"ab"}, pmr_string{"c"}}, {pmr_string{"a"}, pmr_string{"bc"}}, {pmr_string{"ab"}, pmr_string{"c"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  const auto schema = StringOnlyKeySchema<1>::build(input.column_ids, *input.table, 4);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  expect_keys_equal(schema, keys[0], keys[2]);
  expect_keys_not_equal(schema, keys[0], keys[1]);
  expect_unpack_round_trip(schema, definitions, keys, rows);
}

TEST_F(AggregateDYODKeySchemaTest, ContentFillingTheNarrowBlobStaysInline) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{pmr_string{"abc"}, pmr_string{"def"}},
                                                             {pmr_string{"ab"}, pmr_string{"d"}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  const auto schema = StringOnlyKeySchema<1>::build(input.column_ids, *input.table, 6);
  EXPECT_EQ(schema.fixed_part_width(), 8u);

  auto spill = StringSpillBuffer{};
  const auto keys = pack_all_keys(schema, input, spill);

  EXPECT_EQ(read_spill_pointer(keys[0].data(), schema.fixed_part_width()), nullptr);
  expect_keys_not_equal(schema, keys[0], keys[1]);
  expect_unpack_round_trip(schema, definitions, keys, rows);
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

TEST_F(AggregateDYODKeySchemaTest, DictionarySegmentsPackLikeValueSegments) {
  const auto definitions =
      TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}, {"c", DataType::Float, false}};
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'd');
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"abc"}, 1.5f},
                                                             {NullValue{}, pmr_string{"abc"}, -0.0f},
                                                             {1, NullValue{}, 0.0f},
                                                             {2, long_string, 2.5f},
                                                             {1, pmr_string{"abc"}, 1.5f}};
  const auto plain = make_pack_input(definitions, rows);
  const auto encoded = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  resolve_key_schema(plain.column_ids, *plain.table, [&](const auto& schema) {
    auto spill_plain = StringSpillBuffer{};
    auto spill_encoded = StringSpillBuffer{};
    const auto expected = pack_all_keys(schema, plain, spill_plain);
    const auto actual = pack_all_keys(schema, encoded, spill_encoded);
    for (auto row = size_t{0}; row < expected.size(); ++row) {
      expect_keys_equal(schema, expected[row], actual[row]);
    }
  });
}

TEST_F(AggregateDYODKeySchemaTest, ReferenceSegmentsSpanningChunksPackLikeValueSegments) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"x"}},
                                                             {NullValue{}, pmr_string{"y"}},
                                                             {2, pmr_string{""}},
                                                             {1, pmr_string{"x"}},
                                                             {3, pmr_string{"z"}}};
  const auto plain = make_pack_input(definitions, rows);
  const auto reference = to_reference_input(make_pack_input(definitions, rows, ChunkOffset{2}));
  resolve_key_schema(plain.column_ids, *plain.table, [&](const auto& schema) {
    auto spill_plain = StringSpillBuffer{};
    auto spill_reference = StringSpillBuffer{};
    const auto expected = pack_all_keys(schema, plain, spill_plain);
    const auto actual = pack_all_keys(schema, reference, spill_reference);
    for (auto row = size_t{0}; row < expected.size(); ++row) {
      expect_keys_equal(schema, expected[row], actual[row]);
    }
  });
}

TEST_F(AggregateDYODKeySchemaTest, ReferenceSegmentsOverDictionaryChunksPackLikeValueSegments) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"abc"}},
                                                             {NullValue{}, pmr_string{"abc"}},
                                                             {2, NullValue{}},
                                                             {1, pmr_string{"xy"}},
                                                             {3, pmr_string{"abc"}}};
  const auto plain = make_pack_input(definitions, rows);
  const auto reference = to_reference_input(
      make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary}));
  resolve_key_schema(plain.column_ids, *plain.table, [&](const auto& schema) {
    auto spill_plain = StringSpillBuffer{};
    auto spill_reference = StringSpillBuffer{};
    const auto expected = pack_all_keys(schema, plain, spill_plain);
    const auto actual = pack_all_keys(schema, reference, spill_reference);
    for (auto row = size_t{0}; row < expected.size(); ++row) {
      expect_keys_equal(schema, expected[row], actual[row]);
    }
  });
}

TEST_F(AggregateDYODKeySchemaTest, NullRowIdsPackAsNullValues) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}};
  const auto plain = make_pack_input(definitions, {{7}, {NullValue{}}, {8}});

  const auto data = make_pack_input(definitions, {{7}, {8}});
  auto pos_list = std::make_shared<RowIDPosList>();
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});
  pos_list->emplace_back(NULL_ROW_ID);
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  auto reference = PackInput{};
  const auto reference_table = std::make_shared<Table>(definitions, TableType::References);
  reference_table->append_chunk({std::make_shared<ReferenceSegment>(data.table, ColumnID{0}, pos_list)});
  reference.table = reference_table;
  reference.column_ids = plain.column_ids;
  gather_segments(reference);

  const auto schema = NumericShortKeySchema<8>::build(plain.column_ids, *plain.table);
  auto spill = StringSpillBuffer{};
  const auto expected = pack_all_keys(schema, plain, spill);
  const auto actual = pack_all_keys(schema, reference, spill);
  for (auto row = size_t{0}; row < expected.size(); ++row) {
    expect_keys_equal(schema, expected[row], actual[row]);
  }
}

TEST_F(AggregateDYODKeySchemaTest, WindowedDecodeMatchesTheFullDecodeSlice) {
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'w');
  const auto definitions =
      TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}, {"c", DataType::Double, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {1, pmr_string{"abc"}, 1.5}, {NullValue{}, pmr_string{""}, -0.5}, {2, NullValue{}, 2.5},
      {3, long_string, 3.5},       {4, pmr_string{"abc"}, 4.5},         {NullValue{}, NullValue{}, 5.5}};
  const auto input = make_pack_input(definitions, rows);
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    expect_window_matches_full_decode(schema, input, 0, rows.size());
    expect_window_matches_full_decode(schema, input, 2, 5);
    expect_window_matches_full_decode(schema, input, 0, 1);
    expect_window_matches_full_decode(schema, input, rows.size() - 1, rows.size());
  });
}

TEST_F(AggregateDYODKeySchemaTest, WindowedDecodeMatchesOnDictionarySegments) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::Long, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{
      {1, int64_t{10}}, {NullValue{}, int64_t{20}}, {3, int64_t{30}}, {4, int64_t{40}}, {NullValue{}, int64_t{50}}};
  const auto input = make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary});
  resolve_key_schema(input.column_ids, *input.table, [&](const auto& schema) {
    expect_window_matches_full_decode(schema, input, 1, 4);
    expect_window_matches_full_decode(schema, input, 4, 5);
  });
}

TEST_F(AggregateDYODKeySchemaTest, WindowedDecodeMatchesOnSingleChunkReferences) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"abc"}},
                                                             {NullValue{}, pmr_string{"de"}},
                                                             {2, NullValue{}},
                                                             {3, pmr_string{""}},
                                                             {4, pmr_string{"abc"}}};
  const auto plain = to_single_chunk_reference_input(make_pack_input(definitions, rows));
  const auto encoded = to_single_chunk_reference_input(
      make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::Dictionary}));
  resolve_key_schema(plain.column_ids, *plain.table, [&](const auto& schema) {
    expect_window_matches_full_decode(schema, plain, 1, 4);
    expect_window_matches_full_decode(schema, encoded, 1, 4);
    expect_window_matches_full_decode(schema, encoded, 0, 2);
  });
}

TEST_F(AggregateDYODKeySchemaTest, WindowedDecodeMatchesOnSegmentsWithoutDirectAccess) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{1, pmr_string{"abc"}},     {1, pmr_string{"abc"}},
                                                             {NullValue{}, NullValue{}}, {2, pmr_string{"xy"}},
                                                             {3, pmr_string{"xy"}},      {4, pmr_string{"z"}}};
  const auto run_length =
      make_pack_input(definitions, rows, std::nullopt, SegmentEncodingSpec{EncodingType::RunLength});
  const auto reference = to_reference_input(make_pack_input(definitions, rows, ChunkOffset{2}));
  resolve_key_schema(run_length.column_ids, *run_length.table, [&](const auto& schema) {
    expect_window_matches_full_decode(schema, run_length, 2, 5);
    expect_window_matches_full_decode(schema, reference, 2, 5);
    expect_window_matches_full_decode(schema, reference, 5, 6);
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

TEST_F(AggregateDYODKeySchemaTest, ColumnCountMatchesTheGroupByTuple) {
  const auto numeric = make_pack_input({{"a", DataType::Int, false}, {"b", DataType::Long, false}}, {{1, int64_t{2}}});
  EXPECT_EQ(NumericShortKeySchema<12>::build(numeric.column_ids, *numeric.table).column_count(), 2u);

  const auto wide_definitions = TableColumnDefinitions{{"a", DataType::Int, false},
                                                       {"b", DataType::Int, false},
                                                       {"c", DataType::Int, false},
                                                       {"d", DataType::Int, false},
                                                       {"e", DataType::Int, false}};
  const auto wide = make_pack_input(wide_definitions, {{1, 2, 3, 4, 5}});
  EXPECT_EQ(NumericArbitraryKeySchema::build(wide.column_ids, *wide.table).column_count(), 5u);

  const auto mixed =
      make_pack_input({{"a", DataType::Int, false}, {"b", DataType::String, false}}, {{1, pmr_string{"x"}}});
  EXPECT_EQ(MixedKeySchema<4>::build(mixed.column_ids, *mixed.table).column_count(), 2u);

  const auto strings = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"x"}}});
  EXPECT_EQ(StringOnlyKeySchema<4>::build(strings.column_ids, *strings.table).column_count(), 1u);
}

TEST_F(AggregateDYODKeySchemaTest, ReinternedSpilledKeysSurviveSpillBufferReuse) {
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'r');
  const auto definitions = TableColumnDefinitions{
      {"a", DataType::Int, false}, {"b", DataType::String, true}, {"c", DataType::String, false}};
  const auto rows = std::vector<std::vector<AllTypeVariant>>{{7, NullValue{}, long_string}};
  const auto input = make_pack_input(definitions, rows);
  const auto schema = MixedKeySchema<4>::build(input.column_ids, *input.table);

  auto scatter_spill = StringSpillBuffer{};
  auto merge_spill = StringSpillBuffer{};
  auto key = pack_key(schema, input, 0, scatter_spill);
  schema.reintern_spill(key.data(), merge_spill);

  scatter_spill.clear();
  const auto garbage = std::vector<std::byte>(long_string.size(), std::byte{'!'});
  scatter_spill.append(garbage.data(), garbage.size());

  auto other_spill = StringSpillBuffer{};
  const auto reference = pack_key(schema, input, 0, other_spill);
  expect_keys_equal(schema, key, reference);
  expect_unpack_round_trip(schema, definitions, {key}, rows);
}

TEST_F(AggregateDYODKeySchemaTest, ReinternLeavesInlineKeysUntouched) {
  const auto input = make_pack_input({{"a", DataType::String, false}}, {{pmr_string{"inline"}}});
  const auto schema = StringOnlyKeySchema<4>::build(input.column_ids, *input.table);

  auto spill = StringSpillBuffer{};
  auto key = pack_key(schema, input, 0, spill);
  const auto before = key;
  schema.reintern_spill(key.data(), spill);

  EXPECT_EQ(key, before);
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
