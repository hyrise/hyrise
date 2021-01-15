#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "storage/turboPFOR_segment.hpp"
#include "storage/turboPFOR_segment/turboPFOR_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

#include "types.hpp"


namespace opossum {

class TurboPFORTest : public BaseTest {
 
};

template <typename T>
std::shared_ptr<TurboPFORSegment<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = ChunkEncoder::encode_segment(segment, data_type, SegmentEncodingSpec{EncodingType::TurboPFOR});
  return std::dynamic_pointer_cast<TurboPFORSegment<T>>(encoded_segment);
}


TEST_F(TurboPFORTest, BasicTurboPFORWorks) {
    std::shared_ptr<ValueSegment<int32_t>> int_segment = std::make_shared<ValueSegment<int32_t>>(true);
    int_segment->append(2);
    int_segment->append(-1);

    auto segment = compress(int_segment, DataType::Int);
    auto value1 = boost::get<int32_t>((*segment)[0]);
    auto value2 = boost::get<int32_t>((*segment)[1]);
    EXPECT_EQ(value1, 2);
    EXPECT_EQ(value2, -1);
}

}  // namespace opossum
