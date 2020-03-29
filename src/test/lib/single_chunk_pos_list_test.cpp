#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/pos_lists/rowid_pos_list.hpp"
#include "storage/pos_lists/single_chunk_pos_list.hpp"

namespace opossum {

class SingleChunkPosListTest : public BaseTest {
 public:
  void SetUp() override {}
};

TEST_F(SingleChunkPosListTest, EqualityToPosList) {
  auto table = load_table("resources/test_data/tbl/float_int.tbl", 10);

  const auto chunkID = ChunkID{0};
  auto chunk = table->get_chunk(chunkID);

  auto singleChunkPosList = std::make_shared<SingleChunkPosList>(chunkID);
  auto& offsets = singleChunkPosList->get_offsets();

  auto rowIDPosList = std::make_shared<RowIDPosList>();
  rowIDPosList->reserve(chunk->size());

  offsets.reserve(chunk->size());
  for (auto i = ChunkOffset{0}; i < chunk->size(); i++) {
    offsets.push_back(i);
    rowIDPosList->push_back(RowID{chunkID, i});
  }

  EXPECT_TRUE(*singleChunkPosList == *rowIDPosList);
}
}  // namespace opossum
