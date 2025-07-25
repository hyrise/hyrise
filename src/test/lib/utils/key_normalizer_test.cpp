#include "base_test.hpp"
#include "utils/key_normalizer.h"

namespace hyrise {

class KeyNormalizerTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);
    column_definitions.emplace_back("c", DataType::String, true);
    table = std::make_shared<Table>(column_definitions, TableType::Data);

    table->append({AllTypeVariant{42}, AllTypeVariant{5.5f}, AllTypeVariant{"hello"}});
    table->append({AllTypeVariant{-10}, NULL_VALUE, AllTypeVariant{"world"}});
    table->append({AllTypeVariant{100}, AllTypeVariant{-10.0f}, AllTypeVariant{"apple"}});
    table->append({AllTypeVariant{42}, AllTypeVariant{5.5f}, NULL_VALUE});
  }

  std::vector<unsigned char> get_normalized_key(const std::vector<SortColumnDefinition>& sort_definitions,
                                                uint32_t string_prefix_length = 12,
                                                ChunkOffset row_offset = ChunkOffset{0}) const {
    auto [buffer, key_size] = KeyNormalizer::convert_table(table, sort_definitions, string_prefix_length);

    // Extract the key for the specified row
    const auto start = buffer.begin() + (row_offset * key_size);
    const auto end = start + key_size;
    return std::vector<unsigned char>(start, end);
  }

  static std::vector<RowID> get_sorted_row_ids(std::vector<unsigned char>& buffer, uint64_t tuple_key_size) {
    std::vector<const unsigned char*> key_pointers;
    for (size_t i = 0; i < buffer.size(); i += tuple_key_size) {
      key_pointers.push_back(&buffer[i]);
    }

    std::ranges::sort(key_pointers, [tuple_key_size](const auto* a, const auto* b) {
      return memcmp(a, b, tuple_key_size) < 0;
    });

    std::vector<RowID> sorted_row_ids;
    const auto row_id_offset = tuple_key_size - sizeof(RowID);
    for (const auto* key_ptr : key_pointers) {
      sorted_row_ids.push_back(*reinterpret_cast<const RowID*>(key_ptr + row_id_offset));
    }
    return sorted_row_ids;
  }

  std::shared_ptr<Table> table;
};

TEST_F(KeyNormalizerTest, SingleColumnIntegerAsc) {
  const auto sort_definitions =
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst}};

  const auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions);

  // The number of columns in the table is 3, so we expect the tuple_key_size to be:
  // (1 + sizeof(int32_t)) + (1 + sizeof(float)) + (1 + string_prefix_length) + sizeof(RowID)
  // But since we only sort by one column, the key should only contain that column.
  // The convert_table logic seems to create a key based on the columns passed for sorting.
  // So the first calculation should be correct.
  uint32_t expected_key_size = 0;
  for (const auto& sort_col : sort_definitions) {
    const auto data_type = table->column_data_type(sort_col.column);
    if (data_type == DataType::String) {
      expected_key_size += 12 + 1;  // Default prefix + null byte
    } else {
      resolve_data_type(data_type, [&](const auto type) {
        using Type = typename decltype(type)::type;
        expected_key_size += sizeof(Type) + 1;  // Value + null byte
      });
    }
  }
  expected_key_size += sizeof(RowID);

  EXPECT_EQ(tuple_key_size, expected_key_size);
  EXPECT_EQ(buffer.size(), expected_key_size * 4);
}

TEST_F(KeyNormalizerTest, IntegerNormalization) {
  // Positive integer, ascending
  auto key_pos_asc = get_normalized_key({SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst}}, 12,
                                        ChunkOffset{0});  // Row 0, value 42
  // Negative integer, ascending
  auto key_neg_asc = get_normalized_key({SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst}}, 12,
                                        ChunkOffset{1});  // Row 1, value -10

  // In ascending sort, key for -10 should be lexicographically smaller than key for 42
  EXPECT_LT(memcmp(key_neg_asc.data(), key_pos_asc.data(), key_pos_asc.size()), 0);

  // Positive integer, descending
  auto key_pos_desc =
      get_normalized_key({SortColumnDefinition{ColumnID{0}, SortMode::DescendingNullsFirst}}, 12, ChunkOffset{0});
  // Negative integer, descending
  auto key_neg_desc =
      get_normalized_key({SortColumnDefinition{ColumnID{0}, SortMode::DescendingNullsFirst}}, 12, ChunkOffset{1});

  // In descending sort, key for 42 should be lexicographically smaller than key for -10
  EXPECT_LT(memcmp(key_pos_desc.data(), key_neg_desc.data(), key_pos_desc.size()), 0);
}

TEST_F(KeyNormalizerTest, FloatNormalization) {
  // Positive float, ascending
  auto key_pos_asc = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsFirst}}, 12,
                                        ChunkOffset{0});  // Row 0, value 5.5f
  // Negative float, ascending
  auto key_neg_asc = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsFirst}}, 12,
                                        ChunkOffset{2});  // Row 2, value -10.0f

  EXPECT_LT(memcmp(key_neg_asc.data(), key_pos_asc.data(), key_pos_asc.size()), 0);

  // Positive float, descending
  auto key_pos_desc =
      get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsFirst}}, 12, ChunkOffset{0});
  // Negative float, descending
  auto key_neg_desc =
      get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsFirst}}, 12, ChunkOffset{2});

  EXPECT_LT(memcmp(key_pos_desc.data(), key_neg_desc.data(), key_pos_desc.size()), 0);
}

TEST_F(KeyNormalizerTest, NullPrefix) {
  // Test NullsFirst: non-NULL byte (0x01) should be greater than NULL byte (0x00)
  auto key_non_null_first = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsFirst}}, 12,
                                               ChunkOffset{0});  // Not NULL
  auto key_null_first = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsFirst}}, 12,
                                           ChunkOffset{1});  // Is NULL

  EXPECT_EQ(key_null_first[0], 0x00);  // Null prefix for NULLS FIRST
  EXPECT_EQ(key_non_null_first[0], 0x01);
  EXPECT_LT(key_null_first[0], key_non_null_first[0]);

  // Test NullsLast: NULL byte (0x01) should be greater than non-NULL byte (0x00)
  auto key_non_null_last = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsLast}}, 12,
                                              ChunkOffset{0});  // Not NULL
  auto key_null_last = get_normalized_key({SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsLast}}, 12,
                                          ChunkOffset{1});  // Is NULL

  EXPECT_EQ(key_non_null_last[0], 0x00);  // Null prefix for NULLS LAST
  EXPECT_EQ(key_null_last[0], 0x01);
  EXPECT_LT(key_non_null_last[0], key_null_last[0]);
}

TEST_F(KeyNormalizerTest, StringNormalizationAndPadding) {
  const uint32_t prefix_len = 8;
  // "apple"
  auto key_apple = get_normalized_key({SortColumnDefinition{ColumnID{2}, SortMode::AscendingNullsFirst}}, prefix_len,
                                      ChunkOffset{2});
  // "hello"
  auto key_hello = get_normalized_key({SortColumnDefinition{ColumnID{2}, SortMode::AscendingNullsFirst}}, prefix_len,
                                      ChunkOffset{0});

  // "apple" should come before "hello"
  EXPECT_LT(memcmp(key_apple.data(), key_hello.data(), key_apple.size()), 0);

  // Check for correct padding. Key for "apple" should be "apple\0\0\0"
  EXPECT_EQ(key_apple[1 + 5], 0x00);  // 1-byte null prefix + 5 chars
  EXPECT_EQ(key_apple[1 + 6], 0x00);
  EXPECT_EQ(key_apple[1 + 7], 0x00);

  // Check descending order (bitwise NOT)
  auto key_apple_desc = get_normalized_key({SortColumnDefinition{ColumnID{2}, SortMode::DescendingNullsFirst}},
                                           prefix_len, ChunkOffset{2});
  // The first byte of the descending key should be the inverse of the ascending one.
  EXPECT_EQ(key_apple_desc[1], (unsigned char)~'a');
}

TEST_F(KeyNormalizerTest, TwoColumnsIntFloat) {
  const std::vector sort_definitions = {SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst},
                                        SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsLast}};

  const auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions);

  const auto expected_tuple_size = (sizeof(int32_t) + 1) + (sizeof(float) + 1) + sizeof(RowID);
  EXPECT_EQ(tuple_key_size, expected_tuple_size);
}

TEST_F(KeyNormalizerTest, StringColumnDescending) {
  const auto sort_definitions = std::vector{SortColumnDefinition{ColumnID{2}, SortMode::DescendingNullsFirst}};
  const uint32_t string_prefix_length = 8;

  const auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions, string_prefix_length);

  const auto expected_tuple_size = string_prefix_length + 1 + sizeof(RowID);
  EXPECT_EQ(tuple_key_size, expected_tuple_size);
}

TEST_F(KeyNormalizerTest, NullsFirst) {
  // Sort by column 'b' (float) with nulls first
  const auto sort_definitions = std::vector{SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsFirst}};

  auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions);

  const auto sorted_row_ids = get_sorted_row_ids(buffer, tuple_key_size);

  // Expected order for column 'b' (ASC, NULLS FIRST): NULL, -10.0, 5.5, 5.5
  // Corresponding chunk offsets: 1, 2, 0, 3 (or 1, 2, 3, 0 depending on tie-breaking)
  EXPECT_EQ(sorted_row_ids[0].chunk_offset, 1);  // Row with NULL
  EXPECT_EQ(sorted_row_ids[1].chunk_offset, 2);  // Row with -10.0f
}

TEST_F(KeyNormalizerTest, NullsLast) {
  // Sort by column 'b' (float) with nulls last
  const auto sort_definitions = std::vector{SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsLast}};
  auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions);

  const auto sorted_row_ids = get_sorted_row_ids(buffer, tuple_key_size);

  // The last row ID should be from the row with the NULL value.
  EXPECT_EQ(sorted_row_ids.back().chunk_offset, 1);
}

TEST_F(KeyNormalizerTest, ComplexSort) {
  const auto sort_definitions =
      std::vector{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst},   // 'a' ASC
                  SortColumnDefinition{ColumnID{2}, SortMode::DescendingNullsLast}};  // 'c' DESC

  auto [buffer, tuple_key_size] = KeyNormalizer::convert_table(table, sort_definitions);

  const auto sorted_row_ids = get_sorted_row_ids(buffer, tuple_key_size);

  // Expected order of chunk offsets:
  // val(-10, "world") -> offset 1
  // val(42,  "hello") -> offset 0
  // val(42,  NULL)    -> offset 3
  // val(100, "apple") -> offset 2
  const auto expected_offsets = std::vector{ChunkOffset{1}, ChunkOffset{0}, ChunkOffset{3}, ChunkOffset{2}};
  for (size_t i = 0; i < expected_offsets.size(); ++i) {
    EXPECT_EQ(sorted_row_ids[i].chunk_offset, expected_offsets[i]);
  }
}

void print_key(const std::vector<unsigned char>& key, const std::string& message = "") {
  if (!message.empty()) {
    std::cout << message << " ";
  }
  std::cout << "Key (" << key.size() << " bytes): [ ";
  for (const auto& byte : key) {
    std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
  }
  std::cout << std::dec << "]" << std::endl;
}

TEST_F(KeyNormalizerTest, DebugFloats) {
    auto float_table = std::make_shared<Table>(TableColumnDefinitions{{"f", DataType::Float, false}}, TableType::Data);
    float_table->append({-10.5f}); // Should be first
    float_table->append({2.0f});   // Should be third
    float_table->append({0.0f});   // Should be second

    const auto sort_definitions = std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsLast}}};
    auto [normalized_keys, key_size] = KeyNormalizer::convert_table(float_table, sort_definitions);

    print_key({normalized_keys.begin() + 0 * key_size, normalized_keys.begin() + 1 * key_size}, "Key for -10.5f:");
    print_key({normalized_keys.begin() + 1 * key_size, normalized_keys.begin() + 2 * key_size}, "Key for   2.0f:");
    print_key({normalized_keys.begin() + 2 * key_size, normalized_keys.begin() + 3 * key_size}, "Key for   0.0f:");

    std::vector<const unsigned char*> key_pointers;
    for (size_t i = 0; i < normalized_keys.size(); i += key_size) {
        key_pointers.push_back(&normalized_keys[i]);
    }

    std::sort(key_pointers.begin(), key_pointers.end(), [key_size](const auto* a, const auto* b) {
        return std::memcmp(a, b, key_size) < 0;
    });

    RowIDPosList sorted_pos_list;
    for (const auto* ptr : key_pointers) {
        sorted_pos_list.emplace_back(*reinterpret_cast<const RowID*>(ptr + (key_size - sizeof(RowID))));
    }

    const RowIDPosList expected_order = {RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{0}, ChunkOffset{1}}};
    EXPECT_EQ(sorted_pos_list, expected_order);
}

// Test case for signed integers.
TEST_F(KeyNormalizerTest, DebugSignedInts) {
    auto int_table = std::make_shared<Table>(TableColumnDefinitions{{"i", DataType::Int, false}}, TableType::Data);
    int_table->append({5});   // Should be third
    int_table->append({-2});  // Should be first
    int_table->append({0});   // Should be second

    const auto sort_definitions = std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsLast}}};
    auto [normalized_keys, key_size] = KeyNormalizer::convert_table(int_table, sort_definitions);

    print_key({normalized_keys.begin() + 0 * key_size, normalized_keys.begin() + 1 * key_size}, "Key for   5:");
    print_key({normalized_keys.begin() + 1 * key_size, normalized_keys.begin() + 2 * key_size}, "Key for  -2:");
    print_key({normalized_keys.begin() + 2 * key_size, normalized_keys.begin() + 3 * key_size}, "Key for   0:");
    std::cout << "Expected Order: Key for -2 < Key for 0 < Key for 5\n";

    std::vector<const unsigned char*> key_pointers;
    for (size_t i = 0; i < normalized_keys.size(); i += key_size) {
        key_pointers.push_back(&normalized_keys[i]);
    }

    std::sort(key_pointers.begin(), key_pointers.end(), [key_size](const auto* a, const auto* b) {
        return std::memcmp(a, b, key_size) < 0;
    });

    RowIDPosList sorted_pos_list;
    for (const auto* ptr : key_pointers) {
        sorted_pos_list.emplace_back(*reinterpret_cast<const RowID*>(ptr + (key_size - sizeof(RowID))));
    }

    const RowIDPosList expected_order = {RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{0}, ChunkOffset{0}}};
    EXPECT_EQ(sorted_pos_list, expected_order);
}

TEST_F(KeyNormalizerTest, DebugMultiColumnMixedOrder) {
    auto multi_table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::String, false}}, TableType::Data);
    multi_table->append({5, "apple"});  // Sorted: {5, "banana"}, {5, "apple"} -> pos 1, pos 0
    multi_table->append({10, "zoo"});   // Sorted: {10, "zoo"}, {10, "car"} -> pos 3, pos 2
    multi_table->append({5, "banana"});
    multi_table->append({10, "car"});

    // ORDER BY a ASC, b DESC
    const auto sort_definitions = std::vector<SortColumnDefinition>{
        {SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsLast}},
        {SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsLast}}
    };

    auto [normalized_keys, key_size] = KeyNormalizer::convert_table(multi_table, sort_definitions);

    print_key({normalized_keys.begin() + 0 * key_size, normalized_keys.begin() + 1 * key_size}, "Key for {5, 'apple'}:");
    print_key({normalized_keys.begin() + 1 * key_size, normalized_keys.begin() + 2 * key_size}, "Key for {10, 'zoo'}:");
    print_key({normalized_keys.begin() + 2 * key_size, normalized_keys.begin() + 3 * key_size}, "Key for {5, 'banana'}:");
    print_key({normalized_keys.begin() + 3 * key_size, normalized_keys.begin() + 4 * key_size}, "Key for {10, 'car'}:");

    std::vector<const unsigned char*> key_pointers;
    for (size_t i = 0; i < normalized_keys.size(); i += key_size) {
        key_pointers.push_back(&normalized_keys[i]);
    }

    std::sort(key_pointers.begin(), key_pointers.end(), [key_size](const auto* a, const auto* b) {
        return std::memcmp(a, b, key_size) < 0;
    });

    RowIDPosList sorted_pos_list;
    for (const auto* ptr : key_pointers) {
        sorted_pos_list.emplace_back(*reinterpret_cast<const RowID*>(ptr + (key_size - sizeof(RowID))));
    }

    const RowIDPosList expected_order = {RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}}};
    EXPECT_EQ(sorted_pos_list, expected_order);
}

}  // namespace hyrise
