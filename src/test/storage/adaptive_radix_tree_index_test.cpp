#include <algorithm>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "types.hpp"

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp"

namespace opossum {

class AdaptiveRadixTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // we want to custom-build the index, but we have to create an index with a non-empty segment.
    // Therefore we build an index and reset the root.
    dict_segment1 = create_dict_segment_by_type<pmr_string>(DataType::String, {"test"});
    index1 = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment1}));
    index1->_root = nullptr;
    index1->_chunk_offsets.clear();
    /* root   childx    childxx  childxxx  leaf->chunk offsets
     * 01 --->  01 -----> 01 -----> 01 --> 0x00000001u, 0x00000007u
     * 02 -|    02 ---|   02 ---|   02 --> 0x00000002u
     *     |          |-> 01 --||-> 01 --> 0x00000003u
     *     |                   |--> 01 --> 0x00000004u
     *     |                        02 --> 0x00000005u
     *     |----------------------> 01 --> 0x00000006u
     */

    keys1 = {ValueID{0x01010101u}, ValueID{0x01010102u}, ValueID{0x01010201u}, ValueID{0x01020101u},
             ValueID{0x01020102u}, ValueID{0x02010101u}, ValueID{0x01010101u}};
    values1 = {0x00000001u, 0x00000002u, 0x00000003u, 0x00000004u, 0x00000005u, 0x00000006u, 0x00000007u};

    for (size_t i = 0; i < 7; ++i) {
      auto bc = AdaptiveRadixTreeIndex::BinaryComparable(keys1[i]);
      pairs.emplace_back(std::make_pair(bc, values1[i]));
    }
    root = index1->_bulk_insert(pairs);

    std::random_device rd;
    _rng = std::mt19937(rd());
  }

  void _search_elements(std::vector<int32_t>& values) {
    std::uniform_int_distribution<int32_t> uni_integer(0, std::numeric_limits<int32_t>::max());

    auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, values);
    auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

    std::set<int32_t> distinct_values(values.begin(), values.end());

    // create search values from given values + randomly chosen ones
    std::set<int32_t> search_values = distinct_values;
    while (search_values.size() < distinct_values.size() * 2) {
      search_values.insert(uni_integer(_rng));
    }

    for (const auto& search_value : search_values) {
      if (distinct_values.find(search_value) != distinct_values.end()) {
        // match
        EXPECT_NE(index->lower_bound({search_value}), index->upper_bound({search_value}));
      } else {
        // no match
        EXPECT_EQ(index->upper_bound({search_value}), index->lower_bound({search_value}));
      }

      int32_t min = *distinct_values.begin();
      int32_t max = *distinct_values.rbegin();

      EXPECT_EQ(index->upper_bound({min - 1}), index->cbegin());
      EXPECT_EQ(index->upper_bound({max + 1}), index->cend());
    }
  }

  std::shared_ptr<AdaptiveRadixTreeIndex> index1 = nullptr;
  std::shared_ptr<BaseSegment> dict_segment1 = nullptr;
  std::shared_ptr<ARTNode> root = nullptr;
  std::vector<std::pair<AdaptiveRadixTreeIndex::BinaryComparable, ChunkOffset>> pairs;
  std::vector<ValueID> keys1;
  std::vector<ChunkOffset> values1;

  std::mt19937 _rng;
};

TEST_F(AdaptiveRadixTreeIndexTest, BinaryComparableFromChunkOffset) {
  ValueID test_value{0x01020304u};
  AdaptiveRadixTreeIndex::BinaryComparable binary_comparable = AdaptiveRadixTreeIndex::BinaryComparable(test_value);
  EXPECT_EQ(binary_comparable[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(binary_comparable[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(binary_comparable[2], static_cast<uint8_t>(0x03u));
  EXPECT_EQ(binary_comparable[3], static_cast<uint8_t>(0x04u));
  EXPECT_EQ(binary_comparable.size(), 4u);
}

TEST_F(AdaptiveRadixTreeIndexTest, BulkInsert) {
  std::vector<ChunkOffset> expected_chunk_offsets = {0x00000001u, 0x00000007u, 0x00000002u, 0x00000003u,
                                                     0x00000004u, 0x00000005u, 0x00000006u};
  EXPECT_FALSE(std::dynamic_pointer_cast<Leaf>(root));
  EXPECT_EQ(index1->_chunk_offsets, expected_chunk_offsets);

  auto root4 = std::dynamic_pointer_cast<ARTNode4>(root);
  EXPECT_EQ(root4->_partial_keys[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(root4->_partial_keys[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(root4->_partial_keys[2], static_cast<uint8_t>(0xffu));
  EXPECT_EQ(root4->_partial_keys[3], static_cast<uint8_t>(0xffu));

  auto child01 = std::dynamic_pointer_cast<ARTNode4>(root4->_children[0]);
  EXPECT_EQ(child01->_partial_keys[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(child01->_partial_keys[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(child01->_partial_keys[2], static_cast<uint8_t>(0xffu));
  EXPECT_EQ(child01->_partial_keys[3], static_cast<uint8_t>(0xffu));

  auto child0101 = std::dynamic_pointer_cast<ARTNode4>(child01->_children[0]);
  EXPECT_EQ(child0101->_partial_keys[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(child0101->_partial_keys[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(child0101->_partial_keys[2], static_cast<uint8_t>(0xffu));
  EXPECT_EQ(child0101->_partial_keys[3], static_cast<uint8_t>(0xffu));

  auto child010101 = std::dynamic_pointer_cast<ARTNode4>(child0101->_children[0]);
  EXPECT_EQ(child0101->_partial_keys[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(child0101->_partial_keys[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(child0101->_partial_keys[2], static_cast<uint8_t>(0xffu));
  EXPECT_EQ(child0101->_partial_keys[3], static_cast<uint8_t>(0xffu));

  auto leaf01010101 = std::dynamic_pointer_cast<Leaf>(child010101->_children[0]);
  EXPECT_EQ(*(leaf01010101->begin()), 0x00000001u);
  EXPECT_EQ(*(leaf01010101->end()), 0x00000002u);
  EXPECT_EQ(std::distance(leaf01010101->begin(), leaf01010101->end()), 2);
  EXPECT_FALSE(std::find(leaf01010101->begin(), leaf01010101->end(), static_cast<uint8_t>(0x00000001u)) ==
               leaf01010101->end());
  EXPECT_FALSE(std::find(leaf01010101->begin(), leaf01010101->end(), static_cast<uint8_t>(0x00000007u)) ==
               leaf01010101->end());

  auto leaf01010102 = std::dynamic_pointer_cast<Leaf>(child010101->_children[1]);
  EXPECT_EQ(*(leaf01010102->begin()), 0x00000002u);
  EXPECT_EQ(*(leaf01010102->end()), 0x00000003u);
  EXPECT_EQ(std::distance(leaf01010102->begin(), leaf01010102->end()), 1);
  EXPECT_FALSE(std::find(leaf01010102->begin(), leaf01010102->end(), static_cast<uint8_t>(0x00000002u)) ==
               leaf01010102->end());

  auto leaf02 = std::dynamic_pointer_cast<Leaf>(root4->_children[1]);
  EXPECT_EQ(std::distance(leaf02->begin(), leaf02->end()), 1);
  EXPECT_EQ(*(leaf02->begin()), 0x00000006u);
  EXPECT_FALSE(std::find(leaf02->begin(), leaf02->end(), static_cast<uint8_t>(0x00000006u)) == leaf02->end());
}

TEST_F(AdaptiveRadixTreeIndexTest, VectorOfRandomInts) {
  size_t test_size = 10'001;
  std::vector<int32_t> ints(test_size);
  for (auto i = 0u; i < test_size; ++i) {
    ints[i] = i * 2;
  }

  std::shuffle(ints.begin(), ints.end(), _rng);

  auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, ints);
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

  for (auto i : {0, 2, 4, 8, 12, 14, 60, 64, 128, 130, 1024, 1026, 2048, 2050, 4096, 8190, 8192, 8194, 16382, 16384}) {
    EXPECT_EQ((*segment)[*index->lower_bound({i})], AllTypeVariant{i});
    EXPECT_EQ((*segment)[*index->lower_bound({i + 1})], AllTypeVariant{i + 2});
    EXPECT_EQ((*segment)[*index->upper_bound({i})], AllTypeVariant{i + 2});
    EXPECT_EQ((*segment)[*index->upper_bound({i + 1})], AllTypeVariant{i + 2});

    auto expected_lower = i;
    for (auto it = index->lower_bound({i}); it < index->lower_bound({i + 20}); ++it) {
      EXPECT_EQ((*segment)[*it], AllTypeVariant{expected_lower});
      expected_lower += 2;
    }
  }

  // In the following tests, we iterate over all integers from 0 to 3*test_size. The largest elements are known to
  // be of size 2*test_size. When items are within the test_size range and even, we should find the item in the index.
  // If it is odd or above the test range, we should not find the item.
  for (auto search_item = int32_t{0}; search_item < static_cast<int32_t>(3 * test_size); ++search_item) {
    if (search_item % 2 == 0 && search_item < static_cast<int32_t>(2 * test_size)) {
      // all multiples of two within range of `ints` should exist
      EXPECT_NE(index->lower_bound({search_item}), index->cend());
      continue;
    }

    // search for elements not existing
    EXPECT_EQ(index->lower_bound({search_item}), index->upper_bound({search_item}));
  }
}

TEST_F(AdaptiveRadixTreeIndexTest, SimpleTest) {
  std::vector<int32_t> values = {0, 0, 0, 0, 0, 17, 17, 17, 99, std::numeric_limits<int32_t>::max()};

  auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, values);
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseSegment>>({segment}));

  // We check whether the index answer (i.e., position(s) for a search value) is correct
  EXPECT_EQ(*index->cbegin(), 0);
  EXPECT_EQ(*index->lower_bound({0}), 0);
  EXPECT_EQ(*index->upper_bound({0}), 5);
  EXPECT_EQ(*index->lower_bound({17}), 5);
  EXPECT_EQ(*index->upper_bound({17}), 8);
  EXPECT_EQ(*index->lower_bound({99}), 8);
  EXPECT_EQ(*index->upper_bound({99}), 9);
  EXPECT_EQ(*index->lower_bound({std::numeric_limits<int32_t>::max()}), 9);
  EXPECT_EQ(index->upper_bound({std::numeric_limits<int32_t>::max()}), index->cend());
}

/**
* The following two cases try to test two rather extreme situations that both
* test the node overflow handling of the ART implementation:
*   - sparse vector: wide range of values (between 1 and MAX_INT) with large gaps
*   - dense vector: expenential distribution, rounded to integer values
**/
TEST_F(AdaptiveRadixTreeIndexTest, SparseVectorOfRandomInts) {
  size_t test_size = 1'000;
  std::uniform_int_distribution<int32_t> uni(1, std::numeric_limits<int32_t>::max() - 1);

  std::vector<int32_t> values(test_size);
  std::generate(values.begin(), values.end(), [this, &uni]() { return uni(_rng); });

  _search_elements(values);
}

TEST_F(AdaptiveRadixTreeIndexTest, DenseVectorOfRandomInts) {
  size_t test_size = 5'000;
  std::exponential_distribution<double> exp(1.0);

  std::vector<int32_t> values(test_size);
  std::generate(values.begin(), values.end(), [this, &exp]() { return static_cast<int32_t>(exp(_rng)); });

  _search_elements(values);
}

}  // namespace opossum
