#include "base_test.hpp"
#include "gtest/gtest.h"
#include "storage/pos_list.hpp"
#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/maintenance/create_table.hpp"
#include "gtest/gtest.h"
#include "storage/single_chunk_pos_list.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/maintenance/create_table.hpp"

namespace opossum {

    class SingleChunkPosListTest : public BaseTest {
    public:
        void SetUp() override {
        }
    };

    TEST_F(SingleChunkPosListTest, EqualityToPosList) {
        auto table = load_table("resources/test_data/tbl/float_int.tbl", 10);

        const auto chunkID = ChunkID{0};
        auto chunk = table->get_chunk(chunkID);
        
        auto singleChunkPosList = std::make_shared<SingleChunkPosList>(chunkID);
        auto& offsets = singleChunkPosList->get_offsets();

        auto posList = std::make_shared<PosList>();
        posList->reserve(chunk->size());

        offsets.reserve(chunk->size());
        for (auto i = ChunkOffset{0}; i < chunk->size(); i++) {
            offsets.push_back(i);
            posList->push_back(RowID{chunkID,i});
        }

        EXPECT_TRUE(*singleChunkPosList == *posList);

    }
}  // namespace opossum
