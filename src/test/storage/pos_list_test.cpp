#include <limits>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

class PosListTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(PosListTest, CopyEmpty) {
  PosList expected_pos_list{};
  PosList actual_pos_list = expected_pos_list.copy();
  EXPECT_EQ(expected_pos_list, actual_pos_list);
}

TEST_F(PosListTest, CopyFilled) {
  PosList expected_pos_list = {RowID{ChunkID{0}, ChunkOffset{13}}, RowID{ChunkID{0}, ChunkOffset{37}},
                               RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{2}, ChunkOffset{8}},
                               RowID{ChunkID{2}, ChunkOffset{15}}};

  PosList actual_pos_list = expected_pos_list.copy();
  EXPECT_EQ(expected_pos_list, actual_pos_list);
}

TEST_F(PosListTest, CopyFilledDuplicates) {
  PosList expected_pos_list = {RowID{ChunkID{19}, ChunkOffset{95}}, RowID{ChunkID{19}, ChunkOffset{95}},
                               RowID{ChunkID{19}, ChunkOffset{95}}};

  PosList actual_pos_list = expected_pos_list.copy();
  EXPECT_EQ(expected_pos_list, actual_pos_list);
}

}  // namespace opossum
