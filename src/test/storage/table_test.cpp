#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"

TEST(StorageTable, HasOneChunkAfterCreation) {
  opossum::Table t(2);
  EXPECT_EQ(t.chunk_count(), 1u);
}

class TableTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    t = std::make_shared<opossum::Table>(opossum::Table(chunk_size));
    t->add_column("col_1", "int");
    t->add_column("col_2", "string");
  }

  uint32_t chunk_size = 2;
  std::shared_ptr<opossum::Table> t = nullptr;
};

TEST_F(TableTest, AppendToTable) {
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
}

TEST_F(TableTest, Getters) {
  // test col count
  EXPECT_EQ(t->col_count(), 2u);

  // test row count
  EXPECT_EQ(t->row_count(), 0u);
  t->append({4, "Hello,"});
  t->append({6, "world"});
  t->append({3, "!"});
  EXPECT_EQ(t->row_count(), 3u);

  // test chunk count after append
  EXPECT_EQ(t->chunk_count(), 2u);

  // test get_column*
  std::string column_name = t->get_column_name(0);
  EXPECT_EQ(column_name, "col_1");
  std::string column_type = t->get_column_type(0);
  EXPECT_EQ(column_type, "int");

  column_name = t->get_column_name(1);
  EXPECT_EQ(column_name, "col_2");
  column_type = t->get_column_type(1);
  EXPECT_EQ(column_type, "string");

  // TODO: Do we want checks here?
  // EXPECT_THROW(t->get_column_name(2), std::exception);
  // EXPECT_THROW(t->get_column_type(2), std::exception);

  size_t column_id = t->get_column_id_by_name("col_2");
  EXPECT_EQ(column_id, 1u);

  EXPECT_THROW(t->get_column_id_by_name("no_column_name"), std::exception);

  // test get_chunk_size
  size_t chunk_size = t->get_chunk_size();
  EXPECT_EQ(chunk_size, 2u);
}

TEST_F(TableTest, CannotCopyTable) { EXPECT_THROW(opossum::Table t_copy(*t), std::exception); }
