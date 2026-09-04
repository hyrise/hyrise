#include <memory>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "storage/chunk.hpp"
#include "storage/index/table_index_statistics.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace hyrise {

class TableIndexStatisticsTest : public BaseTest {
 protected:
  void SetUp() override {
    auto dummy_values = pmr_vector<int32_t>{42};
    auto segment = std::make_shared<ValueSegment<int32_t>>(std::move(dummy_values));
    auto segments = Segments{segment};

    _chunk_a = std::make_shared<Chunk>(segments);
    _chunk_b = std::make_shared<Chunk>(segments);
  }

  std::shared_ptr<Chunk> _chunk_a;
  std::shared_ptr<Chunk> _chunk_b;
};

TEST_F(TableIndexStatisticsTest, OperatorEquals) {
  // setup identical stats objects
  auto stats_1 = TableIndexStatistics{};
  stats_1.column_ids = {ColumnID{0}, ColumnID{1}};
  stats_1.chunk_ids = {{ChunkID{0}, _chunk_a}, {ChunkID{1}, _chunk_b}};

  auto stats_2 = TableIndexStatistics{};
  stats_2.column_ids = {ColumnID{0}, ColumnID{1}};
  stats_2.chunk_ids = {{ChunkID{0}, _chunk_a}, {ChunkID{1}, _chunk_b}};

  EXPECT_EQ(stats_1, stats_2);

  // modify column_ids to test inequality
  auto stats_different_columns = TableIndexStatistics{};
  stats_different_columns.column_ids = {ColumnID{0}, ColumnID{2}};
  stats_different_columns.chunk_ids = {{ChunkID{0}, _chunk_a}, {ChunkID{1}, _chunk_b}};

  EXPECT_NE(stats_1, stats_different_columns);

  // modify chunk_id to test inequality
  auto stats_different_chunk_ids = TableIndexStatistics{};
  stats_different_chunk_ids.column_ids = {ColumnID{0}, ColumnID{1}};
  stats_different_chunk_ids.chunk_ids = {{ChunkID{99}, _chunk_a}, {ChunkID{1}, _chunk_b}};

  EXPECT_NE(stats_1, stats_different_chunk_ids);
}

}  // namespace hyrise
