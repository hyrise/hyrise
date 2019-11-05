#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsInsertTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsInsertTest, SelfInsert) {
  auto table_name = "test_table";
  auto t = load_table("resources/test_data/tbl/float_int.tbl");
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  Hyrise::get().storage_manager.add_table(table_name, t);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->set_transaction_context(context);

  ins->execute();

  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(t->row_count(), 6u);
  EXPECT_EQ(t->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ(t->get_chunk(ChunkID{1})->size(), 3u);
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_segment(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{0})->get_segment(ColumnID{0}))[0], AllTypeVariant(458.7f));
  EXPECT_EQ((*t->get_chunk(ChunkID{1})->get_segment(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(ChunkID{1})->get_segment(ColumnID{0}))[0], AllTypeVariant(458.7f));

  EXPECT_EQ(t->get_chunk(ChunkID{0})->get_segment(ColumnID{0})->size(), 3u);
  EXPECT_EQ(t->get_chunk(ChunkID{0})->get_segment(ColumnID{1})->size(), 3u);
  EXPECT_EQ(t->get_chunk(ChunkID{1})->get_segment(ColumnID{0})->size(), 3u);
  EXPECT_EQ(t->get_chunk(ChunkID{1})->get_segment(ColumnID{1})->size(), 3u);
}

TEST_F(OperatorsInsertTest, InsertRespectChunkSize) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows, chunk_size = 4
  auto t = load_table("resources/test_data/tbl/int.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  // 10 Rows
  auto t2 = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 4u);
  EXPECT_EQ(t->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ(t->get_chunk(ChunkID{3})->size(), 2u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, MultipleChunks) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows
  auto t = load_table("resources/test_data/tbl/int.tbl", 2u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  // 10 Rows
  auto t2 = load_table("resources/test_data/tbl/10_ints.tbl", 3u);
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(t->get_chunk(ChunkID{6})->size(), 2u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, CompressedChunks) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows
  auto t = load_table("resources/test_data/tbl/int.tbl", 2u);
  Hyrise::get().storage_manager.add_table(t_name, t);
  opossum::ChunkEncoder::encode_all_chunks(t);

  // 10 Rows
  auto t2 = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 7u);
  EXPECT_EQ(t->get_chunk(ChunkID{6})->size(), 2u);
  EXPECT_EQ(t->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, Rollback) {
  auto t_name = "test3";

  auto t = load_table("resources/test_data/tbl/int.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  auto gt1 = std::make_shared<GetTable>(t_name);
  gt1->execute();

  auto ins = std::make_shared<Insert>(t_name, gt1);
  auto context1 = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context1);
  ins->execute();
  context1->rollback();

  auto gt2 = std::make_shared<GetTable>(t_name);
  gt2->execute();
  auto validate = std::make_shared<Validate>(gt2);
  auto context2 = Hyrise::get().transaction_manager.new_transaction_context();
  validate->set_transaction_context(context2);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 3u);
}

TEST_F(OperatorsInsertTest, InsertStringNullValue) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("resources/test_data/tbl/string_with_null.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  auto t2 = load_table("resources/test_data/tbl/string_with_null.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 2u);
  EXPECT_EQ(t->row_count(), 8u);

  auto null_val = (*(t->get_chunk(ChunkID{1})->get_segment(ColumnID{0})))[2];
  EXPECT_TRUE(variant_is_null(null_val));
}

TEST_F(OperatorsInsertTest, InsertIntFloatNullValues) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("resources/test_data/tbl/int_float_with_null.tbl", 3u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  auto t2 = load_table("resources/test_data/tbl/int_float_with_null.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 4u);
  EXPECT_EQ(t->row_count(), 8u);

  auto null_val_int = (*(t->get_chunk(ChunkID{2})->get_segment(ColumnID{0})))[2];
  EXPECT_TRUE(variant_is_null(null_val_int));

  auto null_val_float = (*(t->get_chunk(ChunkID{2})->get_segment(ColumnID{1})))[1];
  EXPECT_TRUE(variant_is_null(null_val_float));
}

TEST_F(OperatorsInsertTest, InsertNullIntoNonNull) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  auto t = load_table("resources/test_data/tbl/int_float.tbl", 3u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  auto t2 = load_table("resources/test_data/tbl/int_float_with_null.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name2, t2);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  EXPECT_THROW(ins->execute(), std::logic_error);
  context->rollback();
}

TEST_F(OperatorsInsertTest, InsertSingleNullFromDummyProjection) {
  auto t_name = "test1";

  auto t = load_table("resources/test_data/tbl/float_with_null.tbl", 4u);
  Hyrise::get().storage_manager.add_table(t_name, t);

  auto dummy_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  dummy_wrapper->execute();

  // 0 + NULL to create an int-NULL
  auto projection = std::make_shared<Projection>(dummy_wrapper, expression_vector(add_(0.0f, null_())));
  projection->execute();

  auto ins = std::make_shared<Insert>(t_name, projection);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  EXPECT_EQ(t->chunk_count(), 2u);
  EXPECT_EQ(t->row_count(), 5u);

  auto null_val = (*(t->get_chunk(ChunkID{1})->get_segment(ColumnID{0})))[0];
  EXPECT_TRUE(variant_is_null(null_val));
}

TEST_F(OperatorsInsertTest, InsertIntoEmptyTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, false);

  const auto target_table =
      std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("target_table", target_table);

  const auto table_int_float = load_table("resources/test_data/tbl/int_float.tbl");

  const auto table_wrapper = std::make_shared<TableWrapper>(table_int_float);
  table_wrapper->execute();

  const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
  auto context = Hyrise::get().transaction_manager.new_transaction_context();
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_TABLE_EQ_ORDERED(target_table, table_int_float);
}

}  // namespace opossum
