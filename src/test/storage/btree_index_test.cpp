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

// In this domain input modeling is explicitly used.
// https://github.com/hyrise/hyrise/wiki/Input-Domain-Modeling

namespace opossum {

class BTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    values = {"hotel", "delta", "frank", "delta", "apple", "charlie", "charlie", "inbox"};
    segment = std::make_shared<ValueSegment<pmr_string>>(values);
    sorted = {"apple", "charlie", "charlie", "delta", "delta", "frank", "hotel", "inbox"};
    index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

    chunk_offsets = &(index->_impl->_chunk_offsets);
  }

  std::vector<pmr_string> values;
  std::vector<pmr_string> sorted;
  std::shared_ptr<BTreeIndex> index = nullptr;
  std::shared_ptr<ValueSegment<pmr_string>> segment = nullptr;

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
// Short String Optimization (SSO) stores strings of a certain size in the pmr_string object itself.
// Only strings exceeding this size (15 for libstdc++ and 22 for libc++) are stored on the heap.

/*
  Test cases:
    MemoryConsumptionVeryShortStringNoNulls
    MemoryConsumptionVeryShortStringNulls
    MemoryConsumptionVeryShortStringMixed
    MemoryConsumptionVeryShortStringEmpty
  Tested functions:
    size_t memory_consumption() const;
  
  |    Characteristic               | Block 1 | Block 2 |
  |---------------------------------|---------|---------|
  |[A] index is empty               |    true |   false |
  |[B] index has NULL positions     |    true |   false |
  |[C] index has non-NULL positions |    true |   false |
  
  Base Choice:
    A2, B1, C1
  Further derived combinations:
    A2, B1, C2 
    A2, B2, C1
   (A1, B1, C1) --infeasible---+
    A1, B2, C2 <-alternative-<-+
*/

// A2, B2, C1
TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortStringNoNulls) {
  values = {"h", "d", "f", "d", "a", "c", "c", "i", "b", "z", "x"};
  segment = std::make_shared<ValueSegment<pmr_string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   840 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +  44 number of non-NULL elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 933
  EXPECT_EQ(index->memory_consumption(), 933u);
#else
  // libc++:
  //   848 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 941
  EXPECT_EQ(index->memory_consumption(), 941u);
#endif
}

// A2, B1, C2
TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortStringNulls) {
  const auto& dict_segment_string_nulls =
      BaseTest::create_dict_segment_by_type<pmr_string>(DataType::String, {std::nullopt, std::nullopt});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_nulls}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    24 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +   0 number of non-NULL elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   8 number of NULL elements (2) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // =  81
  EXPECT_EQ(index->memory_consumption(), 81u);
#else
  // libc++:
  // TODO(anyone) implement this more accurate
  //    ?? (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +   0 number of elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   8 number of NULL elements (2) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  EXPECT_GT(index->memory_consumption(), 57u);
#endif
}

// A2, B1, C1
TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortStringMixed) {
  const auto& dict_segment_string_mixed = BaseTest::create_dict_segment_by_type<pmr_string>(
      DataType::String, {std::nullopt, "h", "d", "f", "d", "a", std::nullopt, std::nullopt, "c", std::nullopt, "c", "i",
                         "b", "z", "x", std::nullopt});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_mixed}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   840 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +  44 number of non-NULL elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +  20 number of NULL elements (5) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 953
  EXPECT_EQ(index->memory_consumption(), 953u);
#else
  // libc++:
  //   848 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +  20 number of NULL elements (5) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 961
  EXPECT_EQ(index->memory_consumption(), 961u);
#endif
}

// A1, B2, C2
TEST_F(BTreeIndexTest, MemoryConsumptionVeryShortStringEmpty) {
  const auto& dict_segment_string_empty = BaseTest::create_dict_segment_by_type<pmr_string>(DataType::String, {});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_empty}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    24 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +   0 number of non-NULL elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // =  73
  EXPECT_EQ(index->memory_consumption(), 73u);
#else
  // libc++:
  // TODO(anyone) implement this more accurate
  //    ?? (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +   0 number of elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  EXPECT_GT(index->memory_consumption(), 49u);
#endif
}

TEST_F(BTreeIndexTest, MemoryConsumptionShortString) {
  ASSERT_GE(pmr_string("").capacity(), 7u)
      << "Short String Optimization (SSO) is expected to hold at least 7 characters";

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   841 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 921
  EXPECT_EQ(index->memory_consumption(), 921u);
#else
  // libc++:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 345
  EXPECT_EQ(index->memory_consumption(), 345u);
#endif
}

TEST_F(BTreeIndexTest, MemoryConsumptionLongString) {
  ASSERT_LE(pmr_string("").capacity(), 22u)
      << "Short String Optimization (SSO) is expected to hold at maximum 22 characters";

  values = {"hotelhotelhotelhotelhotel", "deltadeltadeltadelta",  "frankfrankfrankfrank",  "deltadeltadeltadelta",
            "appleappleappleapple",      "charliecharliecharlie", "charliecharliecharlie", "inboxinboxinboxinbox"};
  segment = std::make_shared<ValueSegment<pmr_string>>(values);
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    576 (reported by cpp_btree implementation)
  // +   24 std::vector<ChunkOffset> object overhead
  // +   32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +   20 "appleappleappleapple"
  // +   21 "charliecharliecharlie"
  // +   20 "deltadeltadeltadelta"
  // +   20 "frankfrankfrankfrank"
  // +   20 "inboxinboxinboxinbox"
  // +   25 "hotelhotelhotelhotelhotel"
  // +   24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +    0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +    1 SegmentIndexType
  // = 1047
  EXPECT_EQ(index->memory_consumption(), 1047u);
#else
  // libc++ Only one string exceeds the reserved space (22 characters) for small strings:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  25 "hotelhotelhotelhotelhotel"
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 SegmentIndexType
  // = 370
  EXPECT_EQ(index->memory_consumption(), 370u);
#endif
}

}  // namespace opossum
