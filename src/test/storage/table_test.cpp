#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"

class TableTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  opossum::Table t{2};
};

TEST_F(TableTest, HasOneChunkAfterCreation) { EXPECT_EQ(t.chunk_count(), 1u); }

TEST_F(TableTest, AppendToTable) {
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
}

TEST_F(TableTest, Getters) {
  // test col count
  EXPECT_EQ(t.col_count(), 2u);

  // test row count
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);

  // test chunk count after append
  EXPECT_EQ(t.chunk_count(), 2u);

  // test get_column*
  EXPECT_EQ(t.get_column_name(0), "col_1");
  EXPECT_EQ(t.get_column_type(0), "int");

  EXPECT_EQ(t.get_column_name(1), "col_2");
  EXPECT_EQ(t.get_column_type(1), "string");

  // TODO: Do we want checks here?
  // EXPECT_THROW(t.get_column_name(2), std::exception);
  // EXPECT_THROW(t.get_column_type(2), std::exception);

  EXPECT_EQ(t.get_column_id_by_name("col_2"), 1u);

  EXPECT_THROW(t.get_column_id_by_name("no_column_name"), std::exception);

  // test get_chunk_size
  EXPECT_EQ(t.get_chunk_size(), 2u);
}
