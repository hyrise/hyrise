#include "base_test.hpp"
#include "storage/segment_encoding_utils.hpp"

namespace hyrise {
class SegmentEncodingUtilsTest : public BaseTest {
};

TEST_F(SegmentEncodingUtilsTest, TestAutoSelectChunkEncodingSpec) {
  EXPECT_EQ(auto_select_chunk_encoding_spec({DataType::Double, DataType::Int, DataType::Int, DataType::String},
                                            {ColumnID{0}, ColumnID{2}}),
            (ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded},
                               SegmentEncodingSpec{EncodingType::FrameOfReference},
                               SegmentEncodingSpec{EncodingType::Unencoded},
                               SegmentEncodingSpec{EncodingType::Dictionary}}));
}

}  // namespace hyrise
