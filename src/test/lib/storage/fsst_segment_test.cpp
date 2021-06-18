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

}  // namespace opossum
