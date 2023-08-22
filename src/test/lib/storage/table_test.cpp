#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "memory/zero_allocator.hpp"
#include "resolve_type.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::String, true);
    table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  }

  static tbb::concurrent_vector<std::shared_ptr<Chunk>, ZeroAllocator<std::shared_ptr<Chunk>>>& get_chunks(
      std::shared_ptr<Table>& table) {
    return table->_chunks;
  }

  std::shared_ptr<Table> table;
  TableColumnDefinitions column_definitions;
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(table->chunk_count(), 0);
  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, "!"});
  EXPECT_EQ(table->chunk_count(), 2);
}

TEST_F(StorageTableTest, GetChunk) {
  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, "!"});
  ASSERT_EQ(table->chunk_count(), 2);
  EXPECT_TRUE(table->get_chunk(ChunkID{0}));
  EXPECT_TRUE(table->get_chunk(ChunkID{1}));
}

TEST_F(StorageTableTest, ColumnCount) {
  EXPECT_EQ(table->column_count(), 2);
}

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(table->row_count(), 0);
  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, "!"});
  EXPECT_EQ(table->row_count(), 3);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(table->column_name(ColumnID{0}), "column_1");
  EXPECT_EQ(table->column_name(ColumnID{1}), "column_2");
  EXPECT_THROW(table->column_name(ColumnID{2}), std::logic_error);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(table->column_data_type(ColumnID{0}), DataType::Int);
  EXPECT_EQ(table->column_data_type(ColumnID{1}), DataType::String);
  EXPECT_THROW(table->column_data_type(ColumnID{2}), std::logic_error);
}

TEST_F(StorageTableTest, GetColumnNullability) {
  EXPECT_FALSE(table->column_is_nullable(ColumnID{0}));
  EXPECT_TRUE(table->column_is_nullable(ColumnID{1}));
  EXPECT_THROW(table->column_is_nullable(ColumnID{2}), std::logic_error);
}

TEST_F(StorageTableTest, GetColumnIDByName) {
  EXPECT_EQ(table->column_id_by_name("column_2"), 1);
  EXPECT_THROW(table->column_id_by_name("no_column_name"), std::logic_error);
}

TEST_F(StorageTableTest, GetChunkSize) {
  EXPECT_EQ(table->target_chunk_size(), 2);
}

TEST_F(StorageTableTest, GetValue) {
  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, "!"});
  table->append({3, NULL_VALUE});
  EXPECT_EQ(*table->get_value<int32_t>(ColumnID{0}, 0), 4);
  EXPECT_EQ(*table->get_value<int32_t>(ColumnID{0}, 2), 3);
  EXPECT_FALSE(table->get_value<pmr_string>(ColumnID{1}, 3));

  EXPECT_EQ(*table->get_value<pmr_string>(ColumnID{1}, 0), "Hello,");
  EXPECT_EQ(*table->get_value<pmr_string>(ColumnID{1}, 1), "world");
  EXPECT_ANY_THROW(*table->get_value<int32_t>(ColumnID{1}, 0));
  EXPECT_ANY_THROW(*table->get_value<int32_t>(ColumnID{3}, 0));

  EXPECT_EQ(*table->get_value<int32_t>("column_1", 0), 4);
  EXPECT_EQ(*table->get_value<pmr_string>("column_2", 2), "!");
  EXPECT_THROW(*table->get_value<int32_t>("column_3", 0), std::logic_error);
}

TEST_F(StorageTableTest, GetRow) {
  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, NULL_VALUE});
  EXPECT_EQ(table->get_row(0), std::vector<AllTypeVariant>({4, "Hello,"}));
  EXPECT_EQ(table->get_row(1), std::vector<AllTypeVariant>({6, "world"}));
  EXPECT_TRUE(variant_is_null(table->get_row(2)[1]));
  EXPECT_THROW(table->get_row(4), std::logic_error);
}

TEST_F(StorageTableTest, GetRows) {
  TableColumnDefinitions column_definitions_nullable{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  table = std::make_shared<Table>(column_definitions_nullable, TableType::Data, ChunkOffset{2});

  table->append({4, "Hello,"});
  table->append({6, "world"});
  table->append({3, "!"});
  table->append({9, NullValue{}});

  const auto rows = table->get_rows();

  ASSERT_EQ(rows.size(), 4);
  EXPECT_EQ(rows.at(0), std::vector<AllTypeVariant>({4, "Hello,"}));
  EXPECT_EQ(rows.at(1), std::vector<AllTypeVariant>({6, "world"}));
  EXPECT_EQ(rows.at(2), std::vector<AllTypeVariant>({3, "!"}));
  EXPECT_EQ(rows.at(3).at(0), AllTypeVariant{9});
  EXPECT_TRUE(variant_is_null(rows.at(3).at(1)));
}

TEST_F(StorageTableTest, FillingUpAChunkFinalizesIt) {
  table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

  table->append({4, "Hello,"});

  const auto chunk = table->get_chunk(ChunkID{0});
  const auto mvcc_data = chunk->mvcc_data();
  ASSERT_TRUE(mvcc_data);
  EXPECT_EQ(mvcc_data->max_begin_cid.load(), MvccData::MAX_COMMIT_ID);
  EXPECT_TRUE(chunk->is_mutable());

  table->append({6, "world"});
  table->append({7, "!"});

  EXPECT_EQ(mvcc_data->max_begin_cid.load(), 0);
  EXPECT_FALSE(chunk->is_mutable());
}

TEST_F(StorageTableTest, AppendsMutableChunkIfLastChunkImmutableOnAppend) {
  table = load_table("resources/test_data/tbl/float_int.tbl", ChunkOffset{2});
  EXPECT_EQ(table->chunk_count(), 2);
  EXPECT_EQ(table->row_count(), 3);

  table->append({13.0f, 27});
  EXPECT_EQ(table->chunk_count(), 3);
}

TEST_F(StorageTableTest, EmplaceChunk) {
  EXPECT_EQ(table->chunk_count(), 0);

  const auto vs_int = std::make_shared<ValueSegment<int>>();
  const auto vs_str = std::make_shared<ValueSegment<pmr_string>>();

  vs_int->append(5);
  vs_str->append("five");

  table->append_chunk({vs_int, vs_str});
  EXPECT_EQ(table->chunk_count(), 1);
}

TEST_F(StorageTableTest, EmplaceEmptyChunk) {
  EXPECT_EQ(table->chunk_count(), 0);

  auto vs_int = std::make_shared<ValueSegment<int>>();
  auto vs_str = std::make_shared<ValueSegment<pmr_string>>();

  table->append_chunk({vs_int, vs_str});
  EXPECT_EQ(table->chunk_count(), 1);
}

TEST_F(StorageTableTest, EmplaceEmptyChunkWhenEmptyExists) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  EXPECT_EQ(table->chunk_count(), 0);

  {
    auto vs_int = std::make_shared<ValueSegment<int>>();
    auto vs_str = std::make_shared<ValueSegment<pmr_string>>();
    table->append_chunk({vs_int, vs_str});
  }

  {
    auto vs_int = std::make_shared<ValueSegment<int>>();
    auto vs_str = std::make_shared<ValueSegment<pmr_string>>();
    EXPECT_THROW(table->append_chunk({vs_int, vs_str}), std::logic_error);
  }

  EXPECT_EQ(table->chunk_count(), 1);
}

TEST_F(StorageTableTest, EmplaceChunkAndAppend) {
  EXPECT_EQ(table->chunk_count(), 0);

  table->append({4, "Hello,"});
  EXPECT_EQ(table->chunk_count(), 1);

  auto vs_int = std::make_shared<ValueSegment<int>>();
  auto vs_str = std::make_shared<ValueSegment<pmr_string>>();

  table->append_chunk(Segments{{vs_int, vs_str}});
  EXPECT_EQ(table->chunk_count(), 2);
}

TEST_F(StorageTableTest, EmplaceChunkDoesNotReplaceIfNumberOfChunksGreaterOne) {
  EXPECT_EQ(table->chunk_count(), 0);

  table->append({4, "Hello,"});

  {
    auto vs_int = std::make_shared<ValueSegment<int>>();
    auto vs_str = std::make_shared<ValueSegment<pmr_string>>();

    vs_int->append(5);
    vs_str->append("World!");

    table->append_chunk({vs_int, vs_str});
    EXPECT_EQ(table->chunk_count(), 2);
  }

  {
    auto vs_int = std::make_shared<ValueSegment<int>>();
    auto vs_str = std::make_shared<ValueSegment<pmr_string>>();

    table->append_chunk({vs_int, vs_str});
    EXPECT_EQ(table->chunk_count(), 3);
  }
}

TEST_F(StorageTableTest, ChunkSizeZeroThrows) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  TableColumnDefinitions column_definitions{};
  EXPECT_THROW(Table(column_definitions, TableType::Data, ChunkOffset{0}), std::logic_error);
}

TEST_F(StorageTableTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  auto mvcc_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});

  const auto empty_memory_usage = mvcc_table->memory_usage(MemoryUsageCalculationMode::Sampled);

  mvcc_table->append({4, "Hello"});
  mvcc_table->append({5, "Hello"});

  EXPECT_GT(mvcc_table->memory_usage(MemoryUsageCalculationMode::Sampled),
            empty_memory_usage + 2 * (sizeof(int) + sizeof(pmr_string)) + sizeof(TransactionID) + 2 * sizeof(CommitID));
}

TEST_F(StorageTableTest, StableChunks) {
  // Tests that pointers to a chunk remain valid even if the table grows (#1463)
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{1});
  table->append({100, "Hello"});

  // The address of the first shared_ptr control object
  const auto& chunks_vector = get_chunks(table);
  const auto first_chunk = &chunks_vector[0];

  for (auto i = 1; i < 10; ++i) {
    table->append({i, "Hello"});
  }

  // The vector should have been resized / expanded by now

  EXPECT_EQ(first_chunk, &chunks_vector[0]);
  EXPECT_EQ((*(*first_chunk)->get_segment(ColumnID{0}))[ChunkOffset{0}], AllTypeVariant{100});
}

TEST_F(StorageTableTest, LastChunkOfReferenceTable) {
  const auto reference_table = std::make_shared<Table>(column_definitions, TableType::References);
  const auto posList = std::make_shared<RowIDPosList>(1);
  const auto segment_int = std::make_shared<ReferenceSegment>(table, ColumnID{0}, posList);
  const auto segment_string = std::make_shared<ReferenceSegment>(table, ColumnID{1}, posList);
  const auto segments = Segments{segment_int, segment_string};
  const auto second_segment = Segments{segment_int, segment_string};
  reference_table->append_chunk(segments);
  reference_table->append_chunk(second_segment);
  const auto last_chunk = reference_table->get_chunk(ChunkID{1});
  EXPECT_EQ(reference_table->last_chunk(), last_chunk);
}

TEST_F(StorageTableTest, CreatePartialHashIndex) {
  auto hash_index = table->get_table_indexes();
  EXPECT_TRUE(hash_index.empty());
  const auto world_string = pmr_string{"World"};
  const auto hello_string = pmr_string{"Hello"};
  table->append({4, hello_string});
  table->append({3, world_string});
  table->append({6, hello_string});
  table->append({7, "!"});
  table->append({8, "?"});
  table->create_partial_hash_index(ColumnID{1}, {ChunkID{0}, ChunkID{1}});
  hash_index = table->get_table_indexes();
  ASSERT_EQ(hash_index.size(), 1);
  const auto created_hash_index = hash_index[0];
  EXPECT_EQ(created_hash_index->get_indexed_column_id(), ColumnID{1});

  auto access_range_equals_with_iterators_hello = [](auto begin, const auto end) {
    EXPECT_EQ(std::distance(begin, end), 2);

    EXPECT_EQ(*begin, RowID(ChunkID{0}, ChunkOffset{0}));
    ++begin;
    EXPECT_EQ(*begin, RowID(ChunkID{1}, ChunkOffset{0}));
  };
  created_hash_index->range_equals_with_iterators(access_range_equals_with_iterators_hello, hello_string);

  auto access_range_equals_with_iterators_world = [](const auto begin, const auto end) {
    EXPECT_EQ(std::distance(begin, end), 1);

    EXPECT_EQ(*begin, RowID(ChunkID{0}, ChunkOffset{1}));
  };
  created_hash_index->range_equals_with_iterators(access_range_equals_with_iterators_world, world_string);
}

TEST_F(StorageTableTest, CreatePartialHashIndexOnEmptyChunks) {
  EXPECT_THROW(table->create_partial_hash_index(ColumnID{0}, {}), std::logic_error);
}

}  // namespace hyrise
