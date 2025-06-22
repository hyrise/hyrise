#include <numa.h>
#include <unistd.h>

#include <cmath>
#include <limits>
#include <memory>
#include <vector>
#include <unordered_map>

#include "base_test.hpp"
#include "memory.hpp"
#include "memory/linear_numa_memory_resource.hpp"
#include "resolve_type.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_utils.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"

namespace {
using MemoryResourcePtrs = std::vector<hyrise::MemoryResource*>;
constexpr uint32_t LARGE_TABLE_ROW_COUNT = 1000000;
constexpr uint32_t LARGE_TABLE_CHUNK_SIZE = 100000;
constexpr uint32_t LARGE_TABLE_BUFFER_SIZE = LARGE_TABLE_ROW_COUNT * 50;
}

namespace hyrise {

class NodeHistogram : public std::unordered_map<NumaNodeID, uint64_t> {
public:

  NodeHistogram(const NumaNodeIDs& node_ids) {
    for (auto& node_id : node_ids) {
      (*this)[node_id] = 0;
    }
  }

  void assert_all_nodes_used() {
    for (auto& [_, count] : *this) {
      ASSERT_GT(count, 0);
    }
  }

  void reset() {
    for (auto& [_, count] : *this) {
      count = 0;
    }
  }

  void print() {
    for (auto& [key, count] : *this) {
      printf("%i: %lu\n", key, count);
    }
  }
};

class MemoryResourcePlacementTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memory::numa_init();
    _valid_node_ids = memory::allowed_memory_node_ids();
  }

  // Relevant for dictionary segments
  void verify_attribute_vector(auto attribute_vector, NodeHistogram& histogram) {
    auto verify_value_ids_func = [&](auto& fixed_width_vector) {
      auto& value_ids = fixed_width_vector->data();
      for (auto value_id = ValueID{0}; value_id < value_ids.size(); ++value_id) {
        const auto actual_node = memory::numa_node_of_address(value_ids.data() + value_id);
        ASSERT_TRUE(histogram.contains(actual_node));
        histogram[actual_node]++;
      }
    };
    if (auto fixed_width_vector_8B = std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint8_t>>(attribute_vector)) {
      verify_value_ids_func(fixed_width_vector_8B);
    } else if (auto fixed_width_vector_16B = std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint16_t>>(attribute_vector)) {
      verify_value_ids_func(fixed_width_vector_16B);
    } else if (auto fixed_width_vector_32B = std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint32_t>>(attribute_vector)) {
      verify_value_ids_func(fixed_width_vector_32B);
    } else {
      ASSERT_TRUE(false);
    }
  }

  template <typename T>
  void verify_segment(const std::shared_ptr<ValueSegment<T>>& value_segment, const NumaNodeIDs expected_numa_nodes) {
    auto histogram = NodeHistogram(expected_numa_nodes);
    // Values
    auto& values = value_segment->values();
    for (auto value_idx = 0; value_idx < values.size(); ++value_idx) {
      const auto actual_node = memory::numa_node_of_address(values.data() + value_idx);
      ASSERT_TRUE(histogram.contains(actual_node));
      histogram[actual_node]++;
    }
    histogram.assert_all_nodes_used();
    // Null values
    // auto& null_values = value_segment->null_values();
    // for (auto null_value_idx = 0; null_value_idx < null_values.size(); ++null_value_idx) {
    //   ASSERT_EQ(memory::numa_node_of_address(null_values.data() + null_value_idx), expected_numa_nodes);
    // }
  }

  template <typename T>
  void verify_segment(const std::shared_ptr<DictionarySegment<T>>& dict_segment, const NumaNodeIDs expected_numa_nodes) {
    // Dict Values
    auto histogram = NodeHistogram(expected_numa_nodes);
    auto& dictionary = *dict_segment->dictionary();
    for (auto value_idx = 0; value_idx < dictionary.size(); ++value_idx) {
      const auto actual_node = memory::numa_node_of_address(dictionary.data() + value_idx);
      ASSERT_TRUE(histogram.contains(actual_node));
      histogram[actual_node]++;
    }
    histogram.assert_all_nodes_used();

    // Attribute vector (value IDs)
    histogram.reset();
    verify_attribute_vector(dict_segment->attribute_vector(), histogram);
    histogram.assert_all_nodes_used();
  }

  void verify_segment(const std::shared_ptr<FixedStringDictionarySegment<pmr_string>>& string_dict_segment,
                      const NumaNodeIDs expected_numa_nodes) {
    // Dict Values
    auto histogram = NodeHistogram(expected_numa_nodes);
    auto& fixed_string_dictionary = *string_dict_segment->fixed_string_dictionary();
    for (auto value_idx = 0; value_idx < fixed_string_dictionary.size(); ++value_idx) {
      auto actual_node = memory::numa_node_of_address(fixed_string_dictionary.data() + value_idx);
      ASSERT_TRUE(histogram.contains(actual_node));
      histogram[actual_node]++;
    }
    histogram.assert_all_nodes_used();

    // Attribute vector (value IDs)
    histogram.reset();
    verify_attribute_vector(string_dict_segment->attribute_vector(), histogram);
  }

  void verify_chunk_migration(const std::shared_ptr<Chunk>& chunk, auto& column_definitions, const std::vector<MemoryResource*>& column_resources) {
    for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      auto resource = column_resources.size() == 1 ? column_resources[0] : column_resources[column_id];
      auto linear_resource = dynamic_cast<LinearNumaMemoryResource*>(resource);
      ASSERT_TRUE(linear_resource);

      const auto expected_numa_nodes = linear_resource->node_ids();
      resolve_data_type(column_definitions[column_id].data_type, [&](const auto data_type) {
        using DataType = typename decltype(data_type)::type;
        auto segment = chunk->get_segment(column_id);
        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<DataType>>(segment)) {
          verify_segment(value_segment, expected_numa_nodes);
          return;
        }
        if (const auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<DataType>>(segment)) {
          verify_segment(dict_segment, expected_numa_nodes);
          return;
        }
        if (const auto str_dict_segment =
                std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment)) {
          verify_segment(str_dict_segment, expected_numa_nodes);
          return;
        }
        ASSERT_TRUE(false);  // Should not be reached.
      });
    }
  }

  void verify_table_migration(auto& table, const std::vector<MemoryResource*>& column_resources) {
    auto& column_definitions = table->column_definitions();
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto chunk = table->get_chunk(chunk_id);
      verify_chunk_migration(chunk, column_definitions, column_resources);
    }
  }

  // Nodes on which data can be placed
  NumaNodeIDs _valid_node_ids;
};

////////// Placement Single Nodes

TEST_F(MemoryResourcePlacementTest, ChunkMigrateValueSegmentSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({4, 40l, 4.4f, 44.44});
    table->append({6, 60l, 6.6f, 66.66});
    EXPECT_EQ(table->chunk_count(), 1);

    auto resources = MemoryResourcePtrs{&resource_0, &resource_0, &resource_1, &resource_1};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_1, &resource_0, &resource_0};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, ChunkMigrateDictSegmentSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({4, 40l, 4.4f, 44.44});
    table->append({6, 60l, 6.6f, 66.66});
    table->append({8, 80l, 8.8f, 88.88});
    EXPECT_EQ(table->chunk_count(), 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}}, SegmentEncodingSpec{EncodingType::Dictionary});

    auto resources = MemoryResourcePtrs{&resource_0, &resource_0, &resource_1, &resource_1};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_1, &resource_0, &resource_0};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, ChunkMigrateFixedStringDictSegmentSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({"Table", "Column"});
    table->append({"Definition", "Type"});
    table->append({"Chunk", "Offset"});
    EXPECT_EQ(table->chunk_count(), 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}}, SegmentEncodingSpec{EncodingType::FixedStringDictionary});

    auto resources = MemoryResourcePtrs{&resource_0, &resource_1};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_0};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsValueSegmentSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({0, 0l, 0.0f, 0.0});
    table->append({1, 10l, 1.1f, 11.11});
    table->append({2, 20l, 2.2f, 22.22});
    table->append({3, 30l, 3.3f, 33.33});
    table->append({4, 40l, 4.4f, 44.44});
    table->append({5, 50l, 5.5f, 55.55});
    table->append({6, 60l, 6.6f, 66.66});
    table->append({7, 70l, 7.7f, 77.77});
    table->append({8, 80l, 8.8f, 88.88});
    table->append({9, 90l, 9.9f, 99.99});
    table->append({10, 100l, 10.10f, 100.100});
    EXPECT_EQ(table->chunk_count(), 6);

    auto resources = MemoryResourcePtrs{&resource_0, &resource_0, &resource_1, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_1, &resource_0, &resource_0};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsDictSegmentSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({0, 0l, 0.0f, 0.0});
    table->append({1, 10l, 1.1f, 11.11});
    table->append({2, 20l, 2.2f, 22.22});
    table->append({3, 30l, 3.3f, 33.33});
    table->append({4, 40l, 4.4f, 44.44});
    table->append({5, 50l, 5.5f, 55.55});
    table->append({6, 60l, 6.6f, 66.66});
    table->append({7, 70l, 7.7f, 77.77});
    table->append({8, 80l, 8.8f, 88.88});
    table->append({9, 90l, 9.9f, 99.99});
    table->append({10, 100l, 10.10f, 100.100});
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
    EXPECT_EQ(table->chunk_count(), 6);

    auto resources = MemoryResourcePtrs{&resource_0, &resource_0, &resource_1, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_1, &resource_0, &resource_0};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsFixedStringDictSingleNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(1024 * 1024, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
    table->append({"a0", "A0"});
    table->append({"b1", "B1"});
    table->append({"c2", "C2"});
    table->append({"d3", "D3"});
    table->append({"e4", "E4"});
    table->append({"f5", "F5"});
    table->append({"g6", "G6"});
    table->append({"h7", "H7"});
    table->append({"i8", "I8"});
    table->append({"j9", "J0"});
    table->append({"k10", "K10"});
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    EXPECT_EQ(table->chunk_count(), 6);

    auto resources = MemoryResourcePtrs{&resource_0, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_1, &resource_0};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

////////// Placement Two Nodes

TEST_F(MemoryResourcePlacementTest, ChunkMigrateValueSegmentTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      table->append({int32_t{value_idx}, int64_t{value_idx * 10}, static_cast<float>(value_idx + 0.1f), static_cast<double>(value_idx * 10 + 0.1)});
    }
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01, &resource_1, &resource_01};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_0, &resource_01, &resource_1};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, ChunkMigrateDictSegmentTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      table->append({int32_t{value_idx}, int64_t{value_idx * 10}, static_cast<float>(value_idx + 0.1f), static_cast<double>(value_idx * 10 + 0.1)});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01, &resource_1, &resource_01};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_0, &resource_01, &resource_1};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, ChunkMigrateFixedStringDictSegmentTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      const auto val = value_idx % 100000;
      table->append({std::to_string(val).c_str(), std::to_string(val * 10).c_str()});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01};
    auto chunk = table->get_chunk(ChunkID{0});
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_1};
    chunk->migrate(resources);
    verify_chunk_migration(chunk, column_definitions, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsValueSegmentTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      table->append({int32_t{value_idx}, int64_t{value_idx * 10}, static_cast<float>(value_idx + 0.1f), static_cast<double>(value_idx * 10 + 0.1)});
    }
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01, &resource_1, &resource_01};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_0, &resource_01, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsDictSegmentTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::Long, false);
    column_definitions.emplace_back("column_3", DataType::Float, false);
    column_definitions.emplace_back("column_4", DataType::Double, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      table->append({int32_t{value_idx}, int64_t{value_idx * 10}, static_cast<float>(value_idx + 0.1f), static_cast<double>(value_idx * 10 + 0.1)});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01, &resource_1, &resource_01};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_0, &resource_01, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsFixedStringDictTwoNodes) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      const auto val = value_idx % 100000;
      table->append({std::to_string(val).c_str(), std::to_string(val * 10).c_str()});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_1};
    table->migrate_columns(resources);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

TEST_F(MemoryResourcePlacementTest, TableMigrateColumnsSingleResource) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  auto resource_01 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0], _valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      const auto val = value_idx % 100000;
      table->append({std::to_string(val).c_str(), std::to_string(val * 10).c_str()});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    auto resources = MemoryResourcePtrs{&resource_0, &resource_01};
    table->migrate_columns({ColumnID{0}}, &resource_0, &resource_01);
    verify_table_migration(table, resources);

    resources = MemoryResourcePtrs{&resource_01, &resource_1};
    table->migrate_columns({ColumnID{0}}, &resource_01, &resource_1);
    verify_table_migration(table, resources);
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

// In this test case, one memory resource is used for all table columns.
TEST_F(MemoryResourcePlacementTest, TableMigrateFixedStringDictOneNode) {
  if (_valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << _valid_node_ids.size() << " but test requires at least 2.";
  }

  auto resource_0 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[0]});
  auto resource_1 = LinearNumaMemoryResource(LARGE_TABLE_BUFFER_SIZE, {_valid_node_ids[1]});
  {
    // Additional scope to ensure that the table and its segments are destroyed before memory resources are destroyed.
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::String, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{LARGE_TABLE_CHUNK_SIZE});
    for (auto value_idx = 0; value_idx < LARGE_TABLE_ROW_COUNT; ++value_idx) {
      const auto val = value_idx % 100000;
      table->append({std::to_string(val).c_str(), std::to_string(val * 10).c_str()});
    }
    table->last_chunk()->set_immutable();
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    EXPECT_EQ(table->chunk_count(), std::ceil(static_cast<double>(LARGE_TABLE_ROW_COUNT)/LARGE_TABLE_CHUNK_SIZE));

    table->migrate(&resource_0);
    verify_table_migration(table, {&resource_0});

    table->migrate(&resource_1);
    verify_table_migration(table, {&resource_1});
    // End of scope destroys the table and its structures (chunks, segments) are destroyed.
  }
  // End of scope destroys memory resources.
}

}  // namespace hyrise