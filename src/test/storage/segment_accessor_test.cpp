#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

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
    vc_int = make_shared_by_data_type<BaseValueSegment, ValueSegment>(DataType::Int, true);
    vc_int->append(4);
    vc_int->append(6);
    vc_int->append(3);
    vc_int->append(NULL_VALUE);

    vc_str = make_shared_by_data_type<BaseValueSegment, ValueSegment>(DataType::String, true);
    vc_str->append("Hello,");
    vc_str->append("world");
    vc_str->append("!");
    vc_str->append(NULL_VALUE);

    dc_int = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
    dc_str = encode_segment(EncodingType::Dictionary, DataType::String, vc_str);

    chunk = std::make_shared<Chunk>(Segments{{vc_int, dc_str}});
    tbl = std::make_shared<Table>(TableColumnDefinitions{TableColumnDefinition{"vc_int", DataType::Int},
                                                         TableColumnDefinition{"dc_str", DataType::String}},
                                  TableType::Data);
    tbl->append_chunk(chunk);

    pos_list = std::make_shared<PosList>(PosList{{
        RowID{ChunkID{0}, ChunkOffset{1}},
        RowID{ChunkID{0}, ChunkOffset{2}},
        RowID{ChunkID{0}, ChunkOffset{0}},
        RowID{ChunkID{0}, ChunkOffset{3}},
        NULL_ROW_ID,
    }});

    rc_int = std::make_shared<ReferenceSegment>(tbl, ColumnID{0}, pos_list);
    rc_str = std::make_shared<ReferenceSegment>(tbl, ColumnID{1}, pos_list);
  }

  std::shared_ptr<BaseValueSegment> vc_int;
  std::shared_ptr<BaseValueSegment> vc_str;
  std::shared_ptr<BaseSegment> dc_int;
  std::shared_ptr<BaseSegment> dc_str;
  std::shared_ptr<BaseSegment> rc_int;
  std::shared_ptr<BaseSegment> rc_str;

  std::shared_ptr<Table> tbl;
  std::shared_ptr<Chunk> chunk;
  std::shared_ptr<PosList> pos_list;
};

TEST_F(SegmentAccessorTest, TestValueSegmentInt) {
  auto vc_int_base_accessor = create_segment_accessor<int>(vc_int);
  ASSERT_NE(vc_int_base_accessor, nullptr);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{2}), 3);
  EXPECT_FALSE(vc_int_base_accessor->access(ChunkOffset{3}).has_value());
}

TEST_F(SegmentAccessorTest, TestValueSegmentString) {
  auto vc_str_accessor = create_segment_accessor<pmr_string>(vc_str);
  ASSERT_NE(vc_str_accessor, nullptr);
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{2}), "!");
  EXPECT_FALSE(vc_str_accessor->access(ChunkOffset{3}).has_value());
}

TEST_F(SegmentAccessorTest, TestDictionarySegmentInt) {
  auto dc_int_accessor = create_segment_accessor<int>(dc_int);
  ASSERT_NE(dc_int_accessor, nullptr);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{2}), 3);
  EXPECT_FALSE(dc_int_accessor->access(ChunkOffset{3}).has_value());
}

TEST_F(SegmentAccessorTest, TestDictionarySegmentString) {
  auto dc_str_accessor = create_segment_accessor<pmr_string>(dc_str);
  ASSERT_NE(dc_str_accessor, nullptr);
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{2}), "!");
  EXPECT_FALSE(dc_str_accessor->access(ChunkOffset{3}).has_value());
}

TEST_F(SegmentAccessorTest, TestReferenceSegmentToValueSegmentInt) {
  auto rc_int_accessor = create_segment_accessor<int>(rc_int);
  ASSERT_NE(rc_int_accessor, nullptr);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{0}), 6);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{1}), 3);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{2}), 4);
  EXPECT_FALSE(rc_int_accessor->access(ChunkOffset{3}).has_value());
  EXPECT_FALSE(rc_int_accessor->access(ChunkOffset{4}).has_value());
}

TEST_F(SegmentAccessorTest, TestReferenceSegmentToDictionarySegmentString) {
  auto rc_str_accessor = create_segment_accessor<pmr_string>(rc_str);
  ASSERT_NE(rc_str_accessor, nullptr);
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{0}), "world");
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{1}), "!");
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{2}), "Hello,");
  EXPECT_FALSE(rc_str_accessor->access(ChunkOffset{3}).has_value());
  EXPECT_FALSE(rc_str_accessor->access(ChunkOffset{4}).has_value());
}

TEST_F(SegmentAccessorTest, TestSingleChunkReferenceSegmentAccessorNull) {
  auto pos_list = PosList{NULL_ROW_ID};
  pos_list.guarantee_single_chunk();

  auto rc_single_chunk =
      std::make_shared<ReferenceSegment>(tbl, ColumnID{1u}, std::make_shared<PosList>(std::move(pos_list)));

  auto rc_single_chunk_accessor = create_segment_accessor<pmr_string>(rc_single_chunk);
  ASSERT_NE(rc_single_chunk_accessor, nullptr);
  EXPECT_FALSE(rc_single_chunk_accessor->access(ChunkOffset{0}).has_value());
}

}  // namespace opossum
