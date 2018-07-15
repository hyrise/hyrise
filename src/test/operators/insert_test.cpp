#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsInsertTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsInsertTest, SelfInsert) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  StorageManager::get().add_table(table_name, t);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();

  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(t->get_chunk(ChunkID{0})->size(), 6u);
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[3], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[3], AllTypeVariant(458.7f));

  EXPECT_EQ(t->get_chunk(ChunkID{0})->get_column(ColumnID{0})->size(), 6u);
  EXPECT_EQ(t->get_chunk(ChunkID{0})->get_column(ColumnID{1})->size(), 6u);
}

TEST_F(OperatorsInsertTest, InsertRespectChunkSize) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows, chunk_size = 4
  auto t = load_table("src/test/tables/int.tbl", 4u);
  StorageManager::get().add_table(t_name, t);

  // 10 Rows
  auto t2 = load_table("src/test/tables/10_ints.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 4u);
  EXPECT_EQ(t->get_chunk(ChunkID{3})->size(), 1u);
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
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{6})->size(), 1u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, CompressedChunks) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows
  auto t = load_table("src/test/tables/int.tbl", 2u);
  StorageManager::get().add_table(t_name, t);
  opossum::ChunkEncoder::encode_all_chunks(t);

  // 10 Rows
  auto t2 = load_table("src/test/tables/10_ints.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{6})->size(), 2u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, Rollback) {
  auto t_name = "test3";

  auto t = load_table("src/test/tables/int.tbl", 4u);
  StorageManager::get().add_table(t_name, t);

  auto gt1 = std::make_shared<GetTable>(t_name);
  gt1->execute();

  auto ins = std::make_shared<Insert>(t_name, gt1);
  auto context1 = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context1);
  ins->execute();
  context1->rollback();

  auto gt2 = std::make_shared<GetTable>(t_name);
  gt2->execute();
  auto validate = std::make_shared<Validate>(gt2);
  auto context2 = TransactionManager::get().new_transaction_context();
  validate->set_transaction_context(context2);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 3u);
}

TEST_F(OperatorsInsertTest, InsertStringNullValue) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("src/test/tables/string_with_null.tbl", 4u);
  StorageManager::get().add_table(t_name, t);

  auto t2 = load_table("src/test/tables/string_with_null.tbl", 4u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 2u);
  EXPECT_EQ(t->row_count(), 8u);

  auto null_val = (*(t->get_chunk(ChunkID{1})->get_column(ColumnID{0})))[2];
  EXPECT_TRUE(variant_is_null(null_val));
}

TEST_F(OperatorsInsertTest, InsertIntFloatNullValues) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("src/test/tables/int_float_with_null.tbl", 3u);
  StorageManager::get().add_table(t_name, t);

  auto t2 = load_table("src/test/tables/int_float_with_null.tbl", 4u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 3u);
  EXPECT_EQ(t->row_count(), 8u);

  auto null_val_int = (*(t->get_chunk(ChunkID{2})->get_column(ColumnID{0})))[0];
  EXPECT_TRUE(variant_is_null(null_val_int));

  auto null_val_float = (*(t->get_chunk(ChunkID{1})->get_column(ColumnID{1})))[2];
  EXPECT_TRUE(variant_is_null(null_val_float));
}

TEST_F(OperatorsInsertTest, InsertNullIntoNonNull) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("src/test/tables/int_float.tbl", 3u);
  StorageManager::get().add_table(t_name, t);

  auto t2 = load_table("src/test/tables/int_float_with_null.tbl", 4u);
  StorageManager::get().add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  EXPECT_THROW(ins->execute(), std::logic_error);
  context->rollback();
}

TEST_F(OperatorsInsertTest, InsertSingleNullFromDummyProjection) {
  auto t_name = "test1";

  auto t = load_table("src/test/tables/float_with_null.tbl", 4u);
  StorageManager::get().add_table(t_name, t);

  auto dummy_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  dummy_wrapper->execute();

  // 0 + NULL to create an int-NULL
  auto projection = std::make_shared<Projection>(dummy_wrapper, expression_vector(add_(0.0f, null_())));
  projection->execute();

  auto ins = std::make_shared<Insert>(t_name, projection);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 2u);
  EXPECT_EQ(t->row_count(), 5u);

  auto null_val = (*(t->get_chunk(ChunkID{1})->get_column(ColumnID{0})))[0];
  EXPECT_TRUE(variant_is_null(null_val));
}

}  // namespace opossum
