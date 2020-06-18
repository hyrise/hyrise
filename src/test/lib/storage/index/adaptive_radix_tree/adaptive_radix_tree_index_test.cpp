#include <algorithm>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "types.hpp"

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp"

namespace opossum {

class AdaptiveRadixTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // we want to custom-build the index, but we have to create an index with a non-empty segment.
    // Therefore we build an index and reset the root.
    _dict_segment1 = create_dict_segment_by_type<pmr_string>(DataType::String, {"test"});
    _index1 =
        std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({_dict_segment1}));
    _index1->_root = nullptr;
    _index1->_chunk_offsets.clear();
    /* _root   childx    childxx  childxxx  leaf->chunk offsets
     * 01 --->  01 -----> 01 -----> 01 --> 0x00000001u, 0x00000007u
     * 02 -|    02 ---|   02 ---|   02 --> 0x00000002u
     *     |          |-> 01 --||-> 01 --> 0x00000003u
     *     |                   |--> 01 --> 0x00000004u
     *     |                        02 --> 0x00000005u
     *     |----------------------> 01 --> 0x00000006u
     */

    _keys1 = {ValueID{0x01010101u}, ValueID{0x01010102u}, ValueID{0x01010201u}, ValueID{0x01020101u},
              ValueID{0x01020102u}, ValueID{0x02010101u}, ValueID{0x01010101u}};
    _values1 = {0x00000001u, 0x00000002u, 0x00000003u, 0x00000004u, 0x00000005u, 0x00000006u, 0x00000007u};

    for (size_t i = 0; i < 7; ++i) {
      auto bc = AdaptiveRadixTreeIndex::BinaryComparable(_keys1[i]);
      _pairs.emplace_back(std::make_pair(bc, _values1[i]));
    }
    _root = _index1->_bulk_insert(_pairs);
  }

  void _search_elements(std::vector<std::optional<int32_t>>& values) {
    auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, values);
    auto index =
        std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({segment}));

    std::set<std::optional<int32_t>> distinct_values(values.begin(), values.end());

    std::set<std::optional<int32_t>> search_values = distinct_values;
    // Add further values to search_values until the size is twice the size of distinct_values.
    for (auto counter = 0u; counter < distinct_values.size(); ++counter) {
      search_values.insert(_unindexed_value_pool[counter % _unindexed_value_pool.size()]);
    }

    for (const auto& search_value : search_values) {
      if (distinct_values.contains(search_value)) {
        // match
        EXPECT_NE(index->lower_bound({*search_value}), index->upper_bound({*search_value}));
      } else {
        // no match
        EXPECT_EQ(index->upper_bound({*search_value}), index->lower_bound({*search_value}));
      }

      const auto begin = distinct_values.begin();
      const auto rbegin = distinct_values.rbegin();

      Assert(*begin && *rbegin, "Optional has no value.");
      int32_t min = **begin;
      int32_t max = **rbegin;

      EXPECT_EQ(index->upper_bound({min - 1}), index->cbegin());
      EXPECT_EQ(index->upper_bound({max + 1}), index->cend());
    }
  }

  // 0, 1, -1, the maximum int32_t value, the minimum int32_t value and 20 randomly generated int32_t values
  const std::array<int32_t, 25> _indexed_value_pool{
      0,           1,           -1,          2147483647, -2147483648, -354718155, -450451703, -125078514, 1173534911,
      -1426766195, -1395715196, -1974007197, 1217037717, -369295213,  912804945,  -650700817, -987310781, 2019494295,
      -142016072,  -547126853,  -1338076362, -474901211, 1039229181,  1059574590, -775950157};

  // 20 randomly generated int32_t values, the intersection of _indexed_value_pool and _unindexed_value_pool is empty
  const std::array<int32_t, 20> _unindexed_value_pool{24841977,    40823842,    -838897495,  717133222,   -2003779882,
                                                      1567779104,  2055744676,  -1237803657, -2138857329, 665953096,
                                                      1765592675,  -1347132114, -991516138,  -131391814,  1179314207,
                                                      -1783302483, -1370034719, 1545498882,  742923039,   1989133269};

  std::shared_ptr<AdaptiveRadixTreeIndex> _index1 = nullptr;
  std::shared_ptr<AbstractSegment> _dict_segment1 = nullptr;
  std::shared_ptr<ARTNode> _root = nullptr;
  std::vector<std::pair<AdaptiveRadixTreeIndex::BinaryComparable, ChunkOffset>> _pairs;
  std::vector<ValueID> _keys1;
  std::vector<ChunkOffset> _values1;
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
  EXPECT_FALSE(std::dynamic_pointer_cast<Leaf>(_root));
  EXPECT_EQ(_index1->_chunk_offsets, expected_chunk_offsets);

  auto _root4 = std::dynamic_pointer_cast<ARTNode4>(_root);
  EXPECT_EQ(_root4->_partial_keys[0], static_cast<uint8_t>(0x01u));
  EXPECT_EQ(_root4->_partial_keys[1], static_cast<uint8_t>(0x02u));
  EXPECT_EQ(_root4->_partial_keys[2], static_cast<uint8_t>(0xffu));
  EXPECT_EQ(_root4->_partial_keys[3], static_cast<uint8_t>(0xffu));

  auto child01 = std::dynamic_pointer_cast<ARTNode4>(_root4->_children[0]);
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

  auto leaf02 = std::dynamic_pointer_cast<Leaf>(_root4->_children[1]);
  EXPECT_EQ(std::distance(leaf02->begin(), leaf02->end()), 1);
  EXPECT_EQ(*(leaf02->begin()), 0x00000006u);
  EXPECT_FALSE(std::find(leaf02->begin(), leaf02->end(), static_cast<uint8_t>(0x00000006u)) == leaf02->end());
}

TEST_F(AdaptiveRadixTreeIndexTest, VectorOfInts) {
  size_t test_size = 10'001;
  std::vector<std::optional<int32_t>> ints(test_size);
  for (auto index = 0u; index < test_size; ++index) {
    ints[index] = index * 2;
  }

  std::vector<std::optional<int32_t>> skewed_ints;
  skewed_ints.reserve(ints.size());

  // Let one iterator run from the beginning and one from the end of the list in each other's direction. Add the
  // iterators' elements alternately to the skewed list.
  auto front_iter = ints.cbegin();
  auto back_iter = ints.cend() - 1;
  const auto half_list_size = ints.size() / 2;
  for (auto counter = 0u; counter < half_list_size; ++counter) {
    skewed_ints.emplace_back(*front_iter);
    skewed_ints.emplace_back(*back_iter);
    ++front_iter;
    --back_iter;
  }
  if (front_iter == back_iter) {  // odd number of list elements
    skewed_ints.emplace_back(*front_iter);
  }

  auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, skewed_ints);
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({segment}));

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
  std::vector<std::optional<int32_t>> values = {0, 0, 0, 0, 0, 17, 17, 17, 99, std::numeric_limits<int32_t>::max()};

  auto segment = create_dict_segment_by_type<int32_t>(DataType::Int, values);
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({segment}));

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
*   - dense vector: exponential distribution, rounded to integer values
**/
TEST_F(AdaptiveRadixTreeIndexTest, SparseVectorOfInts) {
  const auto test_size = 1'000u;
  auto values = std::vector<std::optional<int32_t>>{};
  values.reserve(test_size);

  for (auto value_index = 0u; value_index < test_size; ++value_index) {
    values[value_index] = _indexed_value_pool[value_index % _indexed_value_pool.size()];
  }

  _search_elements(values);
}

TEST_F(AdaptiveRadixTreeIndexTest, DenseVectorOfInts) {
  // Value counters of an exemplary exponential distribution. The index of a counter represents the value.
  auto value_counters = std::array<uint16_t, 8>{3243, 1118, 420, 143, 55, 14, 4, 3};

  auto overall_value_counter = 0u;
  for (const auto counter : value_counters) {
    overall_value_counter += counter;
  }

  std::vector<std::optional<int32_t>> values{};
  values.reserve(overall_value_counter);

  auto values_to_write = overall_value_counter;
  while (values_to_write > 0) {
    // value equals the value counter index
    for (uint8_t value = 0u; value < value_counters.size(); ++value) {
      auto& value_counter = value_counters[value];
      if (value_counter > 0) {
        values.emplace_back(value);
        --value_counter;
        --values_to_write;
      }
    }
  }

  DebugAssert(values.size() == overall_value_counter, "Number of generated values is incorrect.");

  _search_elements(values);
}

}  // namespace opossum
