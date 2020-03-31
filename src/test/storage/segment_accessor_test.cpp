#include <memory>

#include "../base_test.hpp"

#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class SegmentAccessorTest : public BaseTest {
 protected:
  void SetUp() override {
    val_seg_int = std::make_shared<ValueSegment<int>>(true);
    val_seg_int->append(4);
    val_seg_int->append(6);
    val_seg_int->append(3);
    val_seg_int->append(NULL_VALUE);

    val_seg_str = std::make_shared<ValueSegment<pmr_string>>(true);
    val_seg_str->append("Hello,");
    val_seg_str->append("world");
    val_seg_str->append("!");
    val_seg_str->append(NULL_VALUE);

    dict_seg_int =
        ChunkEncoder::encode_segment(val_seg_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    dict_seg_str =
        ChunkEncoder::encode_segment(val_seg_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary});

    tbl =
        std::make_shared<Table>(TableColumnDefinitions{TableColumnDefinition{"val_seg_int", DataType::Int, false},
                                                       TableColumnDefinition{"dict_seg_str", DataType::String, false}},
                                TableType::Data);
    tbl->append_chunk({val_seg_int, dict_seg_str});

    pos_list = std::make_shared<RowIDPosList>(RowIDPosList{{
        RowID{ChunkID{0}, ChunkOffset{1}},
        RowID{ChunkID{0}, ChunkOffset{2}},
        RowID{ChunkID{0}, ChunkOffset{0}},
        RowID{ChunkID{0}, ChunkOffset{3}},
        NULL_ROW_ID,
    }});

    ref_seg_int = std::make_shared<ReferenceSegment>(tbl, ColumnID{0}, pos_list);
    ref_seg_str = std::make_shared<ReferenceSegment>(tbl, ColumnID{1}, pos_list);
  }

  std::shared_ptr<BaseValueSegment> val_seg_int;
  std::shared_ptr<BaseValueSegment> val_seg_str;
  std::shared_ptr<BaseSegment> dict_seg_int;
  std::shared_ptr<BaseSegment> dict_seg_str;
  std::shared_ptr<BaseSegment> ref_seg_int;
  std::shared_ptr<BaseSegment> ref_seg_str;

  std::shared_ptr<Table> tbl;
  std::shared_ptr<Chunk> chunk;
  std::shared_ptr<RowIDPosList> pos_list;
};

TEST_F(SegmentAccessorTest, TestValueSegmentInt) {
  auto val_seg_int_base_accessor = create_segment_accessor<int>(val_seg_int);
  ASSERT_NE(val_seg_int_base_accessor, nullptr);
  EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{2}), 3);
  EXPECT_FALSE(val_seg_int_base_accessor->access(ChunkOffset{3}));
}

TEST_F(SegmentAccessorTest, TestValueSegmentString) {
  auto val_seg_str_accessor = create_segment_accessor<pmr_string>(val_seg_str);
  ASSERT_NE(val_seg_str_accessor, nullptr);
  EXPECT_EQ(val_seg_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(val_seg_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(val_seg_str_accessor->access(ChunkOffset{2}), "!");
  EXPECT_FALSE(val_seg_str_accessor->access(ChunkOffset{3}));
}

TEST_F(SegmentAccessorTest, TestDictionarySegmentInt) {
  auto dict_seg_int_accessor = create_segment_accessor<int>(dict_seg_int);
  ASSERT_NE(dict_seg_int_accessor, nullptr);
  EXPECT_EQ(dict_seg_int_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(dict_seg_int_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(dict_seg_int_accessor->access(ChunkOffset{2}), 3);
  EXPECT_FALSE(dict_seg_int_accessor->access(ChunkOffset{3}));
}

TEST_F(SegmentAccessorTest, TestDictionarySegmentString) {
  auto dict_seg_str_accessor = create_segment_accessor<pmr_string>(dict_seg_str);
  ASSERT_NE(dict_seg_str_accessor, nullptr);
  EXPECT_EQ(dict_seg_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(dict_seg_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(dict_seg_str_accessor->access(ChunkOffset{2}), "!");
  EXPECT_FALSE(dict_seg_str_accessor->access(ChunkOffset{3}));
}

TEST_F(SegmentAccessorTest, TestReferenceSegmentToValueSegmentInt) {
  auto ref_seg_int_accessor = create_segment_accessor<int>(ref_seg_int);
  ASSERT_NE(ref_seg_int_accessor, nullptr);
  EXPECT_EQ(ref_seg_int_accessor->access(ChunkOffset{0}), 6);
  EXPECT_EQ(ref_seg_int_accessor->access(ChunkOffset{1}), 3);
  EXPECT_EQ(ref_seg_int_accessor->access(ChunkOffset{2}), 4);
  EXPECT_FALSE(ref_seg_int_accessor->access(ChunkOffset{3}));
  EXPECT_FALSE(ref_seg_int_accessor->access(ChunkOffset{4}));
}

TEST_F(SegmentAccessorTest, TestReferenceSegmentToDictionarySegmentString) {
  auto ref_seg_str_accessor = create_segment_accessor<pmr_string>(ref_seg_str);
  ASSERT_NE(ref_seg_str_accessor, nullptr);
  EXPECT_EQ(ref_seg_str_accessor->access(ChunkOffset{0}), "world");
  EXPECT_EQ(ref_seg_str_accessor->access(ChunkOffset{1}), "!");
  EXPECT_EQ(ref_seg_str_accessor->access(ChunkOffset{2}), "Hello,");
  EXPECT_FALSE(ref_seg_str_accessor->access(ChunkOffset{3}));
  EXPECT_FALSE(ref_seg_str_accessor->access(ChunkOffset{4}));
}

TEST_F(SegmentAccessorTest, TestSegmentAccessCounterIncrementing) {
  const auto& access_counter = val_seg_int->access_counter;
  EXPECT_EQ(access_counter[SegmentAccessCounter::AccessType::Random], 0ul);

  // Create segment accessor in a new scope to ensure its destructor, which writes the access counters, is called.
  {
    auto val_seg_int_base_accessor = create_segment_accessor<int>(val_seg_int);

    EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{0}), 4);
    EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{1}), 6);
    EXPECT_EQ(val_seg_int_base_accessor->access(ChunkOffset{2}), 3);
  }

  EXPECT_EQ(access_counter[SegmentAccessCounter::AccessType::Random], 3ul);
}

}  // namespace opossum
