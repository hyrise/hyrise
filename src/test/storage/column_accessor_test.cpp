#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/base_column.hpp"
#include "storage/column_accessor.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class ColumnAccessorTest : public BaseTest {
 protected:
  void SetUp() override {
    vc_int = make_shared_by_data_type<BaseValueColumn, ValueColumn>(DataType::Int);
    vc_int->append(4);
    vc_int->append(6);
    vc_int->append(3);

    vc_str = make_shared_by_data_type<BaseValueColumn, ValueColumn>(DataType::String);
    vc_str->append("Hello,");
    vc_str->append("world");
    vc_str->append("!");

    dc_int = encode_column(EncodingType::Dictionary, DataType::Int, vc_int);
    dc_str = encode_column(EncodingType::Dictionary, DataType::String, vc_str);

    chunk = std::make_shared<Chunk>(ChunkColumns{{vc_int, dc_str}});
    tbl = std::make_shared<Table>(TableColumnDefinitions{TableColumnDefinition{"vc_int", DataType::Int},
                                                         TableColumnDefinition{"dc_str", DataType::String}},
                                  TableType::Data);
    tbl->append_chunk(chunk);

    pos_list = std::make_shared<PosList>(PosList{
        {RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{0}, ChunkOffset{0}}}});

    rc_int = std::make_shared<ReferenceColumn>(tbl, ColumnID{0}, pos_list);
    rc_str = std::make_shared<ReferenceColumn>(tbl, ColumnID{1}, pos_list);
  }

  std::shared_ptr<BaseValueColumn> vc_int;
  std::shared_ptr<BaseValueColumn> vc_str;
  std::shared_ptr<BaseColumn> dc_int;
  std::shared_ptr<BaseColumn> dc_str;
  std::shared_ptr<BaseColumn> rc_int;
  std::shared_ptr<BaseColumn> rc_str;

  std::shared_ptr<Table> tbl;
  std::shared_ptr<Chunk> chunk;
  std::shared_ptr<PosList> pos_list;
};

TEST_F(ColumnAccessorTest, TestValueColumnInt) {
  auto vc_int_base_accessor = create_column_accessor<int>(vc_int);
  ASSERT_NE(vc_int_base_accessor, nullptr);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(vc_int_base_accessor->access(ChunkOffset{2}), 3);
}

TEST_F(ColumnAccessorTest, TestValueColumnString) {
  auto vc_str_accessor = create_column_accessor<std::string>(vc_str);
  ASSERT_NE(vc_str_accessor, nullptr);
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(vc_str_accessor->access(ChunkOffset{2}), "!");
}

TEST_F(ColumnAccessorTest, TestDictionaryColumnInt) {
  auto dc_int_accessor = create_column_accessor<int>(dc_int);
  ASSERT_NE(dc_int_accessor, nullptr);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{0}), 4);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{1}), 6);
  EXPECT_EQ(dc_int_accessor->access(ChunkOffset{2}), 3);
}

TEST_F(ColumnAccessorTest, TestDictionaryColumnString) {
  auto dc_str_accessor = create_column_accessor<std::string>(dc_str);
  ASSERT_NE(dc_str_accessor, nullptr);
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{0}), "Hello,");
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{1}), "world");
  EXPECT_EQ(dc_str_accessor->access(ChunkOffset{2}), "!");
}

TEST_F(ColumnAccessorTest, TestReferenceColumnToValueColumnInt) {
  auto rc_int_accessor = create_column_accessor<int>(rc_int);
  ASSERT_NE(rc_int_accessor, nullptr);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{0}), 6);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{1}), 3);
  EXPECT_EQ(rc_int_accessor->access(ChunkOffset{2}), 4);
}

TEST_F(ColumnAccessorTest, TestReferenceColumnToDictionaryColumnString) {
  auto rc_str_accessor = create_column_accessor<std::string>(rc_str);
  ASSERT_NE(rc_str_accessor, nullptr);
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{0}), "world");
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{1}), "!");
  EXPECT_EQ(rc_str_accessor->access(ChunkOffset{2}), "Hello,");
}

}  // namespace opossum
