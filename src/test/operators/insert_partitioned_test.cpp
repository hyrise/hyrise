#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/pqp_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/partitioning/hash_partition_schema.hpp"
#include "storage/partitioning/null_partition_schema.hpp"
#include "storage/partitioning/range_partition_schema.hpp"
#include "storage/partitioning/round_robin_partition_schema.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsInsertPartitionedTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsInsertPartitionedTest, InsertNullPartitioned) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);

  auto target_table_name = "partitioned_table";
  auto p = Table::create_with_layout_from(t, Chunk::MAX_SIZE);
  p->apply_partitioning(std::make_shared<NullPartitionSchema>());
  StorageManager::get().add_table(target_table_name, p);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(target_table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();
  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(p->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[1], AllTypeVariant(123));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[1], AllTypeVariant(456.7f));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[2], AllTypeVariant(1234));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[2], AllTypeVariant(457.7f));

  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{0})->size(), 3u);
  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{1})->size(), 3u);
}

TEST_F(OperatorsInsertPartitionedTest, InsertHashPartitioned) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);

  auto target_table_name = "partitioned_table";
  auto p = Table::create_with_layout_from(t, Chunk::MAX_SIZE);
  p->apply_partitioning(std::make_shared<HashPartitionSchema>(ColumnID{0}, HashFunction(), PartitionID{3}));
  StorageManager::get().add_table(target_table_name, p);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(target_table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();
  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(p->get_chunk(ChunkID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->size(), 1u);
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(1234));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(457.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{1}))[0], AllTypeVariant(123));
  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{0}))[0], AllTypeVariant(456.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));

  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{1})->size(), 1u);
}

TEST_F(OperatorsInsertPartitionedTest, InsertRangePartitioned) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);

  auto target_table_name = "partitioned_table";
  auto p = Table::create_with_layout_from(t, Chunk::MAX_SIZE);
  std::vector<AllTypeVariant> bounds = {457.0f, 458.0f};
  p->apply_partitioning(std::make_shared<RangePartitionSchema>(ColumnID{0}, bounds));
  StorageManager::get().add_table(target_table_name, p);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(target_table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();
  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(p->get_chunk(ChunkID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->size(), 1u);
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(123));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(456.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{1}))[0], AllTypeVariant(1234));
  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{0}))[0], AllTypeVariant(457.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));

  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{1})->size(), 1u);
}

TEST_F(OperatorsInsertPartitionedTest, InsertRoundRobinPartitioned) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);

  auto target_table_name = "partitioned_table";
  auto p = Table::create_with_layout_from(t, Chunk::MAX_SIZE);
  p->apply_partitioning(std::make_shared<RoundRobinPartitionSchema>(PartitionID{3}));
  StorageManager::get().add_table(target_table_name, p);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto ins = std::make_shared<Insert>(target_table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();
  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(p->get_chunk(ChunkID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->size(), 1u);
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(1234));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(457.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{1}))[0], AllTypeVariant(12345));
  EXPECT_EQ((*p->get_chunk(ChunkID{1})->get_column(ColumnID{0}))[0], AllTypeVariant(458.7f));

  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{1}))[0], AllTypeVariant(123));
  EXPECT_EQ((*p->get_chunk(ChunkID{2})->get_column(ColumnID{0}))[0], AllTypeVariant(456.7f));

  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{0})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{1})->get_column(ColumnID{1})->size(), 1u);

  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{0})->size(), 1u);
  EXPECT_EQ(p->get_chunk(ChunkID{2})->get_column(ColumnID{1})->size(), 1u);
}

}  // namespace opossum
