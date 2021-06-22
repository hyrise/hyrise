#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/fsst_segment.hpp"
#include "storage/fsst_segment/fsst_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

#include <iostream>

namespace opossum {

class StorageFSSTSegmentTest : public BaseTest {
 protected:
  //  static constexpr auto row_count = FSSTEncoder::_block_size + size_t{1000u};
  //  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
};

template <typename T>
std::shared_ptr<FSSTSegment<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = ChunkEncoder::encode_segment(segment, data_type, SegmentEncodingSpec{EncodingType::FSST});
  return std::dynamic_pointer_cast<FSSTSegment<T>>(encoded_segment);
}

//TEST_F(StorageFSSTSegmentTest, IdleTest) {
//  auto empty_str_segment = compress(std::make_shared<ValueSegment<pmr_string>>(true), DataType::String);
//}

TEST_F(StorageFSSTSegmentTest, CreateFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab"};
  pmr_vector<bool> null_values = {false};
  FSSTSegment<pmr_string> segment(values, null_values);
}

TEST_F(StorageFSSTSegmentTest, DecompressFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab"};
  FSSTSegment<pmr_string> segment(values, std::nullopt);

  std::optional<pmr_string> value = segment.get_typed_value(ChunkOffset{2});
  ASSERT_EQ(values[2], value.value());
}

TEST_F(StorageFSSTSegmentTest, DecompressNullFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", ""};
  pmr_vector<bool> null_values = {false, false, true};
  FSSTSegment<pmr_string> segment(values, std::optional(null_values));

  std::optional<pmr_string> value = segment.get_typed_value(ChunkOffset{2});
  ASSERT_EQ(std::nullopt, value);
}

TEST_F(StorageFSSTSegmentTest, FSSTSegmentIterableTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", ""};
  pmr_vector<bool> null_values = {false, false, true};
  FSSTSegment<pmr_string> segment(values, std::optional(null_values));

  pmr_vector<SegmentPosition<pmr_string>> collected_values;

  auto segment_iterable = FSSTSegmentIterable(segment);


  segment_iterable.with_iterators([&collected_values](auto it, auto end) {
    while(it != end){
      collected_values.push_back(*it);
      it++;
    }
  });

  for(size_t index=0; index < values.size(); ++index){
    auto segment_position = collected_values.at(index);
    ASSERT_EQ(segment_position.is_null(), null_values.at(index));
    if (!null_values.at(index)){
      ASSERT_EQ(segment_position.value(), values.at(index));
    }
  }
}

}  // namespace opossum
