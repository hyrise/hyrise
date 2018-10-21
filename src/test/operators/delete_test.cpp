#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsDeleteTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_name = "table_a";
    _table = load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE);
    // Delete Operator works with the Storage Manager, so the test table must also be known to the StorageManager
    StorageManager::get().add_table(_table_name, _table);
    _gt = std::make_shared<GetTable>(_table_name);

    _gt->execute();
  }

  std::string _table_name;
  std::shared_ptr<GetTable> _gt;
  std::shared_ptr<Table> _table;

  void helper(bool commit);
};

void OperatorsDeleteTest::helper(bool commit) {
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // Selects two out of three rows.
  auto table_scan = create_table_scan(_gt, ColumnID{1}, PredicateCondition::GreaterThan, "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(_table_name, table_scan);
  delete_op->set_transaction_context(transaction_context);

  delete_op->execute();

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(0u),
            transaction_context->transaction_id());
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(2u),
            transaction_context->transaction_id());

  // Table has three rows initially.
  ASSERT_NE(_table->table_statistics(), nullptr);
  EXPECT_EQ(_table->table_statistics()->row_count(), 3u);

  auto expected_end_cid = CommitID{0u};
  if (commit) {
    transaction_context->commit();
    expected_end_cid = transaction_context->commit_id();

    // Delete successful, one row left.
    EXPECT_EQ(_table->table_statistics()->row_count(), 3u);
    EXPECT_EQ(_table->table_statistics()->approx_valid_row_count(), 1u);
  } else {
    transaction_context->rollback();
    expected_end_cid = MvccData::MAX_COMMIT_ID;

    // Delete rolled back, three rows left.
    EXPECT_EQ(_table->table_statistics()->row_count(), 3u);
  }

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(0u), expected_end_cid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(1u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(2u), expected_end_cid);

  auto expected_tid = commit ? transaction_context->transaction_id() : 0u;

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(0u), expected_tid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(2u), expected_tid);
}

TEST_F(OperatorsDeleteTest, ExecuteAndCommit) { helper(true); }

TEST_F(OperatorsDeleteTest, ExecuteAndAbort) { helper(false); }

TEST_F(OperatorsDeleteTest, DetectDirtyWrite) {
  auto t1_context = TransactionManager::get().new_transaction_context();
  auto t2_context = TransactionManager::get().new_transaction_context();

  auto table_scan1 = create_table_scan(_gt, ColumnID{0}, PredicateCondition::Equals, "123");
  auto expected_result = create_table_scan(_gt, ColumnID{0}, PredicateCondition::NotEquals, "123");
  auto table_scan2 = create_table_scan(_gt, ColumnID{0}, PredicateCondition::LessThan, "1234");

  table_scan1->execute();
  expected_result->execute();
  table_scan2->execute();

  EXPECT_EQ(table_scan1->get_output()->chunk_count(), 1u);
  EXPECT_EQ(table_scan1->get_output()->get_chunk(ChunkID{0})->column_count(), 2u);

  auto delete_op1 = std::make_shared<Delete>(_table_name, table_scan1);
  delete_op1->set_transaction_context(t1_context);

  auto delete_op2 = std::make_shared<Delete>(_table_name, table_scan2);
  delete_op2->set_transaction_context(t2_context);

  delete_op1->execute();
  delete_op2->execute();

  EXPECT_FALSE(delete_op1->execute_failed());
  EXPECT_TRUE(delete_op2->execute_failed());

  // MVCC commit.
  t1_context->commit();
  t2_context->rollback();

  // Get validated table which should have only one row deleted.
  auto t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(t_context);

  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result->get_output());
}

TEST_F(OperatorsDeleteTest, UpdateAfterDeleteFails) {
  auto t1_context = TransactionManager::get().new_transaction_context();
  auto t2_context = TransactionManager::get().new_transaction_context();

  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(t1_context);

  auto validate2 = std::make_shared<Validate>(_gt);
  validate2->set_transaction_context(t2_context);

  validate1->execute();
  validate2->execute();

  auto delete_op = std::make_shared<Delete>(_table_name, validate1);
  delete_op->set_transaction_context(t1_context);

  delete_op->execute();

  t1_context->commit();

  EXPECT_FALSE(delete_op->execute_failed());

  // this update tries to update the values that have been deleted in another transaction and should fail.
  auto update_op = std::make_shared<Update>(_table_name, validate2, validate2);
  update_op->set_transaction_context(t2_context);
  update_op->execute();
  EXPECT_TRUE(update_op->execute_failed());

  t2_context->rollback();
}

TEST_F(OperatorsDeleteTest, DeleteOwnInsert) {
  // We are testing a couple of things here:
  //   (1) When a transaction deletes a row that it inserted itself, it should no longer be visible to itself
  //   (2) When that transaction rolls back, the row should be invisible to a second transaction
  //   (3) The same should be the case if the transaction commits
  // For that purpose, we run the insert, delete, scan routine twice, once where we abort the transaction (inserted
  // and deleted value 456.7), and once where we commit it (inserted and deleted value 457.7)

  for (const auto value : {456.7, 457.7}) {
    auto context = TransactionManager::get().new_transaction_context();

    auto values_to_insert = load_table("src/test/tables/int_float3.tbl", Chunk::MAX_SIZE);
    auto table_name_for_insert = "bla";
    StorageManager::get().add_table(table_name_for_insert, values_to_insert);
    auto insert_get_table = std::make_shared<GetTable>(table_name_for_insert);
    insert_get_table->execute();

    auto insert = std::make_shared<Insert>(_table_name, insert_get_table);
    insert->set_transaction_context(context);
    insert->execute();

    auto validate1 = std::make_shared<Validate>(_gt);
    validate1->set_transaction_context(context);
    validate1->execute();

    auto table_scan1 = create_table_scan(validate1, ColumnID{1}, PredicateCondition::Equals, value);
    table_scan1->execute();
    EXPECT_EQ(table_scan1->get_output()->row_count(), 2);

    auto delete_op = std::make_shared<Delete>(_table_name, table_scan1);
    delete_op->set_transaction_context(context);
    delete_op->execute();

    auto gt = std::make_shared<GetTable>(_table_name);
    gt->execute();

    auto validate2 = std::make_shared<Validate>(_gt);
    validate2->set_transaction_context(context);
    validate2->execute();

    auto table_scan2 = create_table_scan(validate2, ColumnID{1}, PredicateCondition::Equals, value);
    table_scan2->execute();
    EXPECT_EQ(table_scan2->get_output()->row_count(), 0);

    if (value == 456.7) {
      context->rollback();
    } else {
      context->commit();
    }

    StorageManager::get().drop_table(table_name_for_insert);
  }

  {
    auto context = TransactionManager::get().new_transaction_context();

    auto gt = std::make_shared<GetTable>(_table_name);
    gt->execute();

    auto validate1 = std::make_shared<Validate>(_gt);
    validate1->set_transaction_context(context);
    validate1->execute();

    auto expected_result = load_table("src/test/tables/int_float_deleted.tbl", Chunk::MAX_SIZE);

    EXPECT_TABLE_EQ_UNORDERED(validate1->get_output(), expected_result);

    context->rollback();
  }
}

}  // namespace opossum
