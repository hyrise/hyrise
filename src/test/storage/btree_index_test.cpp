#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "types.hpp"

namespace opossum {

class BTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    values = {"hotel", "delta", "frank", "delta", "apple", "charlie", "charlie", "inbox"};
    segment = std::make_shared<ValueSegment<std::string>>(values);
    sorted = {"apple", "charlie", "charlie", "delta", "delta", "frank", "hotel", "inbox"};
    index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

    chunk_offsets = &(index->_impl->_chunk_offsets);
  }

  std::vector<std::string> values;
  std::vector<std::string> sorted;
  std::shared_ptr<BTreeIndex> index = nullptr;
  std::shared_ptr<ValueSegment<std::string>> segment = nullptr;

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

TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortString) {
  values = {"h", "d", "f", "d", "a", "c", "c", "i"};
  segment = std::make_shared<ValueSegment<std::string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

  EXPECT_EQ(index->memory_consumption(), 320u);
}

TEST_F(BTreeIndexTest, MemoryConsumptionShortString) {
  ASSERT_GE(std::string("").capacity(), 6u)
      << "Short String Optimization (SSO) is expected to hold at least 7 characters";

  EXPECT_EQ(index->memory_consumption(), 320u);
}

TEST_F(BTreeIndexTest, MemoryConsumptionLongString) {
  ASSERT_LT(std::string("").capacity(), 20u)
      << "Short String Optimization (SSO) is expected to hold less than 20 characters";

  values = {"hotelhotelhotelhotelhotel", "deltadeltadeltadelta",  "frankfrankfrankfrank",  "deltadeltadeltadelta",
            "appleappleappleapple",      "charliecharliecharlie", "charliecharliecharlie", "inboxinboxinboxinbox"};
  segment = std::make_shared<ValueSegment<std::string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

  // Index usage:
  //   320 (reported by cpp_btree implementation)
  // +  20 "appleappleappleapple"
  // +  21 "charliecharliecharlie"
  // +  20 "deltadeltadeltadelta"
  // +  20 "frankfrankfrankfrank"
  // +  25 "hotelhotelhotelhotelhotel"
  // +  20 "inboxinboxinboxinbox"
  // = 446

  EXPECT_EQ(index->memory_consumption(), 446u);
}

}  // namespace opossum
