#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/insert.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsInsertTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsInsertTest, SelfInsert) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", 0u);
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  StorageManager::get().add_table(table_name, t);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = std::make_shared<TransactionContext>(1, 1);
  ins->set_transaction_context(context);

  ins->execute();

  // Check that row has been inserted.
  EXPECT_EQ(t->get_chunk(ChunkID{0}).size(), 6u);
  EXPECT_EQ((*t->get_chunk(ChunkID{0}).get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{0}).get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));
  EXPECT_EQ((*t->get_chunk(ChunkID{0}).get_column(ColumnID{1}))[3], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{0}).get_column(ColumnID{0}))[3], AllTypeVariant(458.7f));

  EXPECT_EQ(t->get_chunk(ChunkID{0}).get_column(ColumnID{0})->size(), 6u);
  EXPECT_EQ(t->get_chunk(ChunkID{0}).get_column(ColumnID{1})->size(), 6u);
}

TEST_F(OperatorsInsertTest, InsertRespectChunkSize) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows, column_size = 4
  auto t = load_table("src/test/tables/int.tbl", 4u);
  StorageManager::get().add_table(t_name, t);

  // 10 Rows
  auto t2 = load_table("src/test/tables/10_ints.tbl", 0u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = std::make_shared<TransactionContext>(1, 1);
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_EQ(t->chunk_count(), 4u);
  EXPECT_EQ(t->get_chunk(ChunkID{3}).size(), 1u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, MultipleChunks) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows
  auto t = load_table("src/test/tables/int.tbl", 2u);
  StorageManager::get().add_table(t_name, t);

  // 10 Rows
  auto t2 = load_table("src/test/tables/10_ints.tbl", 3u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = std::make_shared<TransactionContext>(1, 1);
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{6}).size(), 1u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, CompressedChunks) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows
  auto t = load_table("src/test/tables/int.tbl", 2u);
  StorageManager::get().add_table(t_name, t);
  opossum::DictionaryCompression::compress_table(*t);

  // 10 Rows
  auto t2 = load_table("src/test/tables/10_ints.tbl", 0u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = std::make_shared<TransactionContext>(1, 1);
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{6}).size(), 2u);
  EXPECT_EQ(t->row_count(), 13u);
}

}  // namespace opossum
