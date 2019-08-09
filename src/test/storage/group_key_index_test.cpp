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
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

class GroupKeyIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    auto value_segment_str = std::make_shared<ValueSegment<pmr_string>>(true);
    //                                      //  position
    value_segment_str->append(NULL_VALUE);  //  0
    value_segment_str->append("hotel");     //  1
    value_segment_str->append("delta");     //  2
    value_segment_str->append("frank");     //  3
    value_segment_str->append("delta");     //  4
    value_segment_str->append(NULL_VALUE);  //  5
    value_segment_str->append(NULL_VALUE);  //  6
    value_segment_str->append("apple");     //  7
    value_segment_str->append("charlie");   //  8
    value_segment_str->append("charlie");   //  9
    value_segment_str->append("inbox");     // 10
    value_segment_str->append(NULL_VALUE);  // 11

    dict_segment =
        encode_and_compress_segment(value_segment_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary});

    index = std::make_shared<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment}));

    index_offsets = &(index->_index_offsets);
    index_postings = &(index->_index_postings);
  }

  std::shared_ptr<GroupKeyIndex> index = nullptr;
  std::shared_ptr<BaseSegment> dict_segment = nullptr;

  /**
   * Use pointers to inner data structures of CompositeGroupKeyIndex in order to bypass the
   * private scope. In order to minimize the friend classes of CompositeGroupKeyIndex the fixture
   * is used as proxy. Since the variables are set in setup() references are not possible.
   */
  std::vector<ChunkOffset>* index_offsets;
  std::vector<ChunkOffset>* index_postings;
};

TEST_F(GroupKeyIndexTest, IndexOffsets) {
  auto expected_offsets = std::vector<ChunkOffset>{0, 1, 3, 5, 6, 7, 8, 12};
  EXPECT_EQ(expected_offsets, *index_offsets);
}

TEST_F(GroupKeyIndexTest, IndexMemoryConsumption) {
  // expected memory consumption:
  //  - `_indexed_segments`, shared pointer          ->  16 byte
  //  - `_index_offsets`, 8 elements, each 4 byte    ->  32 byte
  //  - `_index_postings`, 12 elements, each 4 byte  ->  48 byte
  //  - sum                                          ->  96 byte
  EXPECT_EQ(index->memory_consumption(), 96u);
}

TEST_F(GroupKeyIndexTest, IndexPostings) {
  // check if there are no duplicates in postings
  auto distinct_values = std::unordered_set<ChunkOffset>(index_postings->begin(), index_postings->end());
  EXPECT_TRUE(distinct_values.size() == index_postings->size());

  // check if the correct postings are present for each value-id
  auto expected_postings = std::vector<std::unordered_set<ChunkOffset>>{
      {7}, {8, 9}, {8, 9}, {2, 4}, {2, 4}, {3}, {1}, {10}, {0, 5, 6, 11}, {0, 5, 6, 11}, {0, 5, 6, 11}, {0, 5, 6, 11}};

  for (size_t i = 0; i < index_postings->size(); ++i) {
    EXPECT_EQ(1u, expected_postings[i].count(index_postings->at(i)));
  }
}

TEST_F(GroupKeyIndexTest, IteratorBeginEnd) {
  EXPECT_EQ(index->cbegin(), index_postings->cbegin());
  EXPECT_EQ(index->cend(), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->null_cbegin(), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->null_cend(), index_postings->cend());
  EXPECT_EQ(index->lower_bound({"inbox"}), index_postings->cbegin() + 7u);
  EXPECT_EQ(index->upper_bound({"inbox"}), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->lower_bound({"hyrise"}), index_postings->cbegin() + 7u);
  EXPECT_EQ(index->upper_bound({"hyrise"}), index_postings->cbegin() + 7u);
  EXPECT_EQ(index->lower_bound({"lamp"}), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->upper_bound({"lamp"}), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->lower_bound({NULL_VALUE}), index_postings->cbegin() + 8u);
  EXPECT_EQ(index->upper_bound({NULL_VALUE}), index_postings->cend());
}

}  // namespace opossum
