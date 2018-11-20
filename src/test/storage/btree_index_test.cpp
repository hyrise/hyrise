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

// The following tests contain switches for different implementations of the stdlib.
// Short String Optimization (SSO) stores strings of a certain size in the std::string object itself.
// Only strings exceeding this size (15 for libstdc++ and 22 for libc++) are stored on the heap.

TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortString) {
  values = {"h", "d", "f", "d", "a", "c", "c", "i", "b", "z", "x"};
  segment = std::make_shared<ValueSegment<std::string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

// Index memory consumption depends on implementation of std::string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   848 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // =  916
  EXPECT_EQ(index->memory_consumption(), 916u);
#else
  // libc++:
  //   808 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // =  876
  EXPECT_EQ(index->memory_consumption(), 876u);
#endif
}

TEST_F(BTreeIndexTest, MemoryConsumptionShortString) {
  ASSERT_GE(std::string("").capacity(), 7u)
      << "Short String Optimization (SSO) is expected to hold at least 7 characters";

// Index memory consumption depends on implementation of std::string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // =  320
  EXPECT_EQ(index->memory_consumption(), 320u);
#else
  // libc++:
  //   248 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // =  304
  EXPECT_EQ(index->memory_consumption(), 304u);
#endif
}

TEST_F(BTreeIndexTest, MemoryConsumptionLongString) {
  ASSERT_LE(std::string("").capacity(), 22u)
      << "Short String Optimization (SSO) is expected to hold at maximum 22 characters";

  values = {"hotelhotelhotelhotelhotel", "deltadeltadeltadelta",  "frankfrankfrankfrank",  "deltadeltadeltadelta",
            "appleappleappleapple",      "charliecharliecharlie", "charliecharliecharlie", "inboxinboxinboxinbox"};
  segment = std::make_shared<ValueSegment<std::string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

// Index memory consumption depends on implementation of std::string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  20 "appleappleappleapple"
  // +  21 "charliecharliecharlie"
  // +  20 "deltadeltadeltadelta"
  // +  20 "frankfrankfrankfrank"
  // +  20 "inboxinboxinboxinbox"
  // +  25 "hotelhotelhotelhotelhotel"
  // =  446
  EXPECT_EQ(index->memory_consumption(), 446u);
#else
  // libc++ Only one string exceeds the reserved space (22 characters) for small strings:
  //   248 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  25 "hotelhotelhotelhotelhotel"
  // =  329
  EXPECT_EQ(index->memory_consumption(), 329u);
#endif
}

}  // namespace opossum
