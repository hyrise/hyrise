#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/variable_length_key_proxy.hpp"
#include "types.hpp"

namespace {
opossum::VariableLengthKey create_key(uint16_t value) {
  auto result = opossum::VariableLengthKey(sizeof(uint16_t));
  result |= value;
  return result;
}

std::vector<opossum::VariableLengthKey> to_vector(const opossum::VariableLengthKeyStore& keys) {
  auto result = std::vector<opossum::VariableLengthKey>(keys.size());
  std::copy(keys.cbegin(), keys.cend(), result.begin());
  return result;
}

testing::AssertionResult is_contained_in(opossum::ChunkOffset value, const std::set<opossum::ChunkOffset>& set) {
  if (set.find(value) == set.end()) {
    return testing::AssertionFailure() << testing::PrintToString(set) << " does not contain " << value;
  } else {
    return testing::AssertionSuccess() << testing::PrintToString(set) << " contains " << value;
  }
}

void EXPECT_POSITION_LIST_EQ(const std::vector<std::set<opossum::ChunkOffset>>& expected,
                             const std::vector<opossum::ChunkOffset>& actual) {
  std::set<opossum::ChunkOffset> distinct_expected_positions = {};
  for (const auto& expectation_for_position : expected) {
    distinct_expected_positions.insert(expectation_for_position.begin(), expectation_for_position.end());
  }

  auto distinct_actual_positions = std::set<opossum::ChunkOffset>(actual.begin(), actual.end());
  EXPECT_EQ(distinct_expected_positions, distinct_actual_positions);

  for (size_t entry = 0; entry < expected.size(); ++entry) {
    EXPECT_TRUE(is_contained_in(actual[entry], expected[entry]));
  }
}
}  // namespace

namespace opossum {

class CompositeGroupKeyIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    _segment_int = create_dict_segment_by_type<int32_t>(DataType::Int, {2, 1, 0, 1, 0, 3, 2, 3});
    _segment_str = create_dict_segment_by_type<pmr_string>(
        DataType::String, {"hotel", "delta", "frank", "delta", "apple", "charlie", "charlie", "inbox"});

    _index_int_str = std::make_shared<CompositeGroupKeyIndex>(
        std::vector<std::shared_ptr<const BaseSegment>>{_segment_int, _segment_str});
    _index_str_int = std::make_shared<CompositeGroupKeyIndex>(
        std::vector<std::shared_ptr<const BaseSegment>>{_segment_str, _segment_int});

    _keys_int_str = &(_index_int_str->_keys);
    _keys_str_int = &(_index_str_int->_keys);

    _offsets_int_str = &(_index_int_str->_key_offsets);
    _offsets_str_int = &(_index_str_int->_key_offsets);

    _position_list_int_str = &(_index_int_str->_position_list);
    _position_list_str_int = &(_index_str_int->_position_list);
  }

 protected:
  std::shared_ptr<CompositeGroupKeyIndex> _index_int_str;
  std::shared_ptr<CompositeGroupKeyIndex> _index_str_int;
  std::shared_ptr<BaseSegment> _segment_int;
  std::shared_ptr<BaseSegment> _segment_str;

  /**
   * Use pointers to inner data structures of CompositeGroupKeyIndex in order to bypass the
   * private scope. In order to minimize the friend classes of CompositeGroupKeyIndex the fixture
   * is used as proxy. Since the variables are set in setup(), references are not possible.
   */
  VariableLengthKeyStore* _keys_int_str;
  VariableLengthKeyStore* _keys_str_int;

  std::vector<ChunkOffset>* _offsets_int_str;
  std::vector<ChunkOffset>* _offsets_str_int;

  std::vector<ChunkOffset>* _position_list_int_str;
  std::vector<ChunkOffset>* _position_list_str_int;
};

TEST_F(CompositeGroupKeyIndexTest, ConcatenatedKeys) {
  auto expected_int_str =
      std::vector<VariableLengthKey>{create_key(0x0000), create_key(0x0003), create_key(0x0102), create_key(0x0201),
                                     create_key(0x0204), create_key(0x0301), create_key(0x0305)};
  auto expected_str_int =
      std::vector<VariableLengthKey>{create_key(0x0000), create_key(0x0102), create_key(0x0103), create_key(0x0201),
                                     create_key(0x0300), create_key(0x0402), create_key(0x0503)};

  EXPECT_EQ(expected_int_str, to_vector(*_keys_int_str));
  EXPECT_EQ(expected_str_int, to_vector(*_keys_str_int));
}

TEST_F(CompositeGroupKeyIndexTest, Offsets) {
  auto expected_int_str = std::vector<ChunkOffset>{0, 1, 2, 4, 5, 6, 7};
  auto expected_str_int = std::vector<ChunkOffset>{0, 1, 2, 3, 5, 6, 7};

  EXPECT_EQ(expected_int_str, *_offsets_int_str);
  EXPECT_EQ(expected_str_int, *_offsets_str_int);
}

TEST_F(CompositeGroupKeyIndexTest, PositionList) {
  auto expected_int_str = std::vector<std::set<ChunkOffset>>{{4}, {2}, {1, 3}, {1, 3}, {6}, {0}, {5}, {7}};
  auto expected_str_int = std::vector<std::set<ChunkOffset>>{{4}, {6}, {5}, {1, 3}, {1, 3}, {2}, {0}, {7}};

  EXPECT_POSITION_LIST_EQ(expected_int_str, *_position_list_int_str);
  EXPECT_POSITION_LIST_EQ(expected_str_int, *_position_list_str_int);
}

}  // namespace opossum
