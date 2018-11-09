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
#include "types.hpp"

namespace opossum {

class GroupKeyIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    dict_segment = BaseTest::create_dict_segment_by_type<std::string>(
        DataType::String, {"hotel", "delta", "frank", "delta", "apple", "charlie", "charlie", "inbox"});
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
  std::vector<std::size_t>* index_offsets;
  std::vector<ChunkOffset>* index_postings;
};

TEST_F(GroupKeyIndexTest, IndexOffsets) {
  auto expected_offsets = std::vector<size_t>{0, 1, 3, 5, 6, 7, 8};
  EXPECT_EQ(expected_offsets, *index_offsets);
}

TEST_F(GroupKeyIndexTest, IndexMemoryConsumption) { EXPECT_EQ(index->memory_consumption(), 104u); }

TEST_F(GroupKeyIndexTest, IndexPostings) {
  // check if there are no duplicates in postings
  auto distinct_values = std::unordered_set<ChunkOffset>(index_postings->begin(), index_postings->end());
  EXPECT_TRUE(distinct_values.size() == index_postings->size());

  // check if the correct postings are present for each value-id
  auto expected_postings =
      std::vector<std::unordered_set<ChunkOffset>>{{4}, {5, 6}, {5, 6}, {1, 3}, {1, 3}, {2}, {0}, {7}};

  for (size_t i = 0; i < index_postings->size(); ++i) {
    EXPECT_EQ(1u, expected_postings[i].count(index_postings->at(i)));
  }
}

}  // namespace opossum
