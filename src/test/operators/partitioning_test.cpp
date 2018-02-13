#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/partitioning.hpp"
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

class OperatorsPartitioningTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsPartitioningTest, PartitionRoundRobin) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);

  auto part = std::make_shared<Partitioning>(table_name, std::make_shared<RoundRobinPartitionSchema>(PartitionID{3}));
  auto context = TransactionManager::get().new_transaction_context();
  part->set_transaction_context(context);

  part->execute();
  context->commit();

  auto p = StorageManager::get().get_table(table_name);

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

TEST_F(OperatorsPartitioningTest, PartitionRange) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);


  std::vector<AllTypeVariant> bounds = {457.0f, 458.0f};
  auto part = std::make_shared<Partitioning>(table_name, std::make_shared<RangePartitionSchema>(ColumnID{0}, bounds));
  auto context = TransactionManager::get().new_transaction_context();
  part->set_transaction_context(context);

  part->execute();
  context->commit();

  auto p = StorageManager::get().get_table(table_name);

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

TEST_F(OperatorsPartitioningTest, PartitionHash) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);


  auto part = std::make_shared<Partitioning>(table_name, std::make_shared<HashPartitionSchema>(ColumnID{0}, HashFunction(), PartitionID{3}));
  auto context = TransactionManager::get().new_transaction_context();
  part->set_transaction_context(context);

  part->execute();
  context->commit();

  auto p = StorageManager::get().get_table(table_name);

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

TEST_F(OperatorsPartitioningTest, RemovePartitioning) {
  auto table_name = "test_table";
  auto t = load_table("src/test/tables/float_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, t);


  auto part = std::make_shared<Partitioning>(table_name, std::make_shared<HashPartitionSchema>(ColumnID{0}, HashFunction(), PartitionID{3}));
  auto context = TransactionManager::get().new_transaction_context();
  part->set_transaction_context(context);

  part->execute();
  context->commit();

  auto unpart = std::make_shared<Partitioning>(table_name, std::make_shared<NullPartitionSchema>());
  auto context_unpart = TransactionManager::get().new_transaction_context();
  unpart->set_transaction_context(context_unpart);

  unpart->execute();
  context_unpart->commit();

  auto p = StorageManager::get().get_table(table_name);

  EXPECT_EQ(p->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[0], AllTypeVariant(1234));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[0], AllTypeVariant(457.7f));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[1], AllTypeVariant(123));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[1], AllTypeVariant(456.7f));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{1}))[2], AllTypeVariant(12345));
  EXPECT_EQ((*p->get_chunk(ChunkID{0})->get_column(ColumnID{0}))[2], AllTypeVariant(458.7f));
}

}  // namespace opossum
