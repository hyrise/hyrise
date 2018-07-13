#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/index/b_tree/b_tree_index.hpp"
#include "../lib/types.hpp"

namespace opossum {

class BTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    values = {"hotel", "delta", "frank", "delta", "apple", "charlie", "charlie", "inbox"};
    column = std::make_shared<ValueColumn<std::string>>(values);
    sorted = {"apple", "charlie", "charlie", "delta", "delta", "frank", "hotel", "inbox"};
    index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseColumn>>({column}));

    chunk_offsets = &(index->_impl->_chunk_offsets);
  }

  std::vector<std::string> values;
  std::vector<std::string> sorted;
  std::shared_ptr<BTreeIndex> index = nullptr;
  std::shared_ptr<ValueColumn<std::string>> column = nullptr;

  /**
   * Use pointers to inner data structures of BTreeIndex in order to bypass the
   * private scope. Since the variable is set in setup() references are not possible.
   */
  std::vector<ChunkOffset>* chunk_offsets;
};

TEST_F(BTreeIndexTest, ChunkOffsets) {
  for (size_t i = 0; i < values.size(); i++) {
    EXPECT_EQ(values[chunk_offsets->at(i)], sorted[i]);
  }
}

TEST_F(BTreeIndexTest, IndexProbes) {
  auto begin = index->cbegin();
  EXPECT_EQ(index->lower_bound({"apple"}) - begin, 0);
  EXPECT_EQ(index->upper_bound({"apple"}) - begin, 1);

  EXPECT_EQ(index->lower_bound({"charlie"}) - begin, 1);
  EXPECT_EQ(index->upper_bound({"charlie"}) - begin, 3);

  EXPECT_EQ(index->lower_bound({"delta"}) - begin, 3);
  EXPECT_EQ(index->upper_bound({"delta"}) - begin, 5);

  EXPECT_EQ(index->lower_bound({"frank"}) - begin, 5);
  EXPECT_EQ(index->upper_bound({"frank"}) - begin, 6);

  EXPECT_EQ(index->lower_bound({"hotel"}) - begin, 6);
  EXPECT_EQ(index->upper_bound({"hotel"}) - begin, 7);

  EXPECT_EQ(index->lower_bound({"inbox"}) - begin, 7);
  EXPECT_EQ(index->upper_bound({"inbox"}) - begin, 8);
}

}  // namespace opossum
