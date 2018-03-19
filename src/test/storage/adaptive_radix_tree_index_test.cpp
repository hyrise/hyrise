#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "types.hpp"

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp"

namespace opossum {

class AdaptiveRadixTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // we want to custom-build the index, but we have to create an index with a non-empty column.
    // Therefore we build an index and reset the root.
    dict_col1 = create_dict_column_by_type<std::string>(DataType::String, {"test"});
    index1 = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseColumn>>({dict_col1}));
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
  }

  std::shared_ptr<AdaptiveRadixTreeIndex> index1 = nullptr;
  std::shared_ptr<BaseColumn> dict_col1 = nullptr;
  std::shared_ptr<ARTNode> root = nullptr;
  std::vector<std::pair<AdaptiveRadixTreeIndex::BinaryComparable, ChunkOffset>> pairs;
  std::vector<ValueID> keys1;
  std::vector<ChunkOffset> values1;
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
  std::vector<int> ints(10001);
  for (auto i = 0u; i < ints.size(); ++i) {
    ints[i] = i * 2;
  }

  std::random_device rd;
  std::mt19937 random_generator(rd());
  std::shuffle(ints.begin(), ints.end(), random_generator);

  auto column = create_dict_column_by_type<int>(DataType::Int, ints);
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const BaseColumn>>({column}));

  for (auto i : {0, 2, 4, 8, 12, 14, 60, 64, 128, 130, 1024, 1026, 2048, 2050, 4096, 8190, 8192, 8194, 16382, 16384}) {
    EXPECT_EQ((*column)[*index->lower_bound({i})], AllTypeVariant{i});
    EXPECT_EQ((*column)[*index->lower_bound({i + 1})], AllTypeVariant{i + 2});
    EXPECT_EQ((*column)[*index->upper_bound({i})], AllTypeVariant{i + 2});
    EXPECT_EQ((*column)[*index->upper_bound({i + 1})], AllTypeVariant{i + 2});

    auto expected_lower = i;
    for (auto it = index->lower_bound({i}); it < index->lower_bound({i + 20}); ++it) {
      EXPECT_EQ((*column)[*it], AllTypeVariant{expected_lower});
      expected_lower += 2;
    }
  }
  EXPECT_EQ(index->upper_bound({99999}), index->cend());
}

}  // namespace opossum
