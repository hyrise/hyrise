#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    cxlumn_definitions.emplace_back("col_1", DataType::Int);
    cxlumn_definitions.emplace_back("col_2", DataType::String);
    t = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2);
  }

  std::shared_ptr<Table> t;
  TableCxlumnDefinitions cxlumn_definitions;
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t->chunk_count(), 0u);
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
  EXPECT_EQ(t->chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
  ASSERT_EQ(t->chunk_count(), 2u);
  EXPECT_NE(t->get_chunk(ChunkID{0}), nullptr);
  EXPECT_NE(t->get_chunk(ChunkID{1}), nullptr);
}

TEST_F(StorageTableTest, ColCount) { EXPECT_EQ(t->cxlumn_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t->row_count(), 0u);
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
  EXPECT_EQ(t->row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t->cxlumn_name(CxlumnID{0}), "col_1");
  EXPECT_EQ(t->cxlumn_name(CxlumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t->cxlumn_name(CxlumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t->cxlumn_data_type(CxlumnID{0}), DataType::Int);
  EXPECT_EQ(t->cxlumn_data_type(CxlumnID{1}), DataType::String);
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t->cxlumn_data_type(CxlumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetCxlumnIDByName) {
  EXPECT_EQ(t->cxlumn_id_by_name("col_2"), 1u);
  EXPECT_THROW(t->cxlumn_id_by_name("no_cxlumn_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t->max_chunk_size(), 2u); }

TEST_F(StorageTableTest, GetValue) {
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
  ASSERT_EQ(t->get_value<int>(CxlumnID{0}, 0u), 4);
  EXPECT_EQ(t->get_value<int>(CxlumnID{0}, 2u), 3);
  ASSERT_FALSE(t->get_value<std::string>(CxlumnID{1}, 0u).compare("Hello,"));
  ASSERT_FALSE(t->get_value<std::string>(CxlumnID{1}, 2u).compare("!"));
  EXPECT_THROW(t->get_value<int>(CxlumnID{3}, 0u), std::exception);
}

TEST_F(StorageTableTest, ShrinkingMvccDataHasNoSideEffects) {
  t = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2, UseMvcc::Yes);

  t->append({4, "Hello,"});
  t->append({6, "world"});

  auto chunk = t->get_chunk(ChunkID{0});

  const auto values = std::vector<CommitID>{1u, 2u};

  {
    // acquiring mvcc_data locks them
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    mvcc_data->tids[0u] = values[0u];
    mvcc_data->tids[1u] = values[1u];
    mvcc_data->begin_cids[0u] = values[0u];
    mvcc_data->begin_cids[1u] = values[1u];
    mvcc_data->end_cids[0u] = values[0u];
    mvcc_data->end_cids[1u] = values[1u];
  }

  const auto previous_size = chunk->size();

  chunk->get_scoped_mvcc_data_lock()->shrink();

  ASSERT_EQ(previous_size, chunk->size());
  ASSERT_TRUE(chunk->has_mvcc_data());

  auto new_mvcc_data = chunk->get_scoped_mvcc_data_lock();

  for (auto i = 0u; i < chunk->size(); ++i) {
    EXPECT_EQ(new_mvcc_data->tids[i], values[i]);
    EXPECT_EQ(new_mvcc_data->begin_cids[i], values[i]);
    EXPECT_EQ(new_mvcc_data->end_cids[i], values[i]);
  }
}

TEST_F(StorageTableTest, EmplaceChunk) {
  EXPECT_EQ(t->chunk_count(), 0u);

  std::shared_ptr<BaseSegment> vs_int = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::Int);
  std::shared_ptr<BaseSegment> vs_str = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::String);

  t->append_chunk({vs_int, vs_str});
  EXPECT_EQ(t->chunk_count(), 1u);
}

TEST_F(StorageTableTest, EmplaceChunkAndAppend) {
  EXPECT_EQ(t->chunk_count(), 0u);

  t->append({4, "Hello,"});
  EXPECT_EQ(t->chunk_count(), 1u);
  std::shared_ptr<BaseSegment> vs_int = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::Int);
  std::shared_ptr<BaseSegment> vs_str = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::String);
  t->append_chunk(Segments{{vs_int, vs_str}});
  EXPECT_EQ(t->chunk_count(), 2u);
}

TEST_F(StorageTableTest, EmplaceChunkDoesNotReplaceIfNumberOfChunksGreaterOne) {
  EXPECT_EQ(t->chunk_count(), 0u);

  t->append({4, "Hello,"});
  std::shared_ptr<BaseSegment> vs_int = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::Int);
  std::shared_ptr<BaseSegment> vs_str = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::String);
  t->append_chunk({vs_int, vs_str});
  EXPECT_EQ(t->chunk_count(), 2u);

  std::shared_ptr<BaseSegment> vs_int2 = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::Int);
  std::shared_ptr<BaseSegment> vs_str2 = make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::String);
  t->append_chunk({vs_int, vs_str});
  EXPECT_EQ(t->chunk_count(), 3u);
}

TEST_F(StorageTableTest, ChunkSizeZeroThrows) {
  TableCxlumnDefinitions cxlumn_definitions{};
  EXPECT_THROW(Table(cxlumn_definitions, TableType::Data, 0), std::logic_error);
}

TEST_F(StorageTableTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  auto mvcc_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2);

  const auto empty_memory_usage = mvcc_table->estimate_memory_usage();

  mvcc_table->append({4, "Hello"});
  mvcc_table->append({5, "Hello"});

  EXPECT_GT(mvcc_table->estimate_memory_usage(), empty_memory_usage + 2 * (sizeof(int) + sizeof(std::string)) +
                                                     sizeof(TransactionID) + 2 * sizeof(CommitID));
}

}  // namespace opossum
