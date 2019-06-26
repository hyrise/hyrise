#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsDeleteTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_name = "table_a";
    _table2_name = "table_b";
    _table = load_table("resources/test_data/tbl/int_float.tbl");
    _table2 = load_table("resources/test_data/tbl/int_int3.tbl", 3);

    // Delete Operator works with the Storage Manager, so the test table must also be known to the StorageManager
    Hyrise::get().storage_manager.add_table(_table_name, _table);
    Hyrise::get().storage_manager.add_table(_table2_name, _table2);

    _gt = std::make_shared<GetTable>(_table_name);
    _gt->execute();
  }

  std::string _table_name, _table2_name;
  std::shared_ptr<GetTable> _gt;
  std::shared_ptr<Table> _table, _table2;

  void helper(bool commit);
};

void OperatorsDeleteTest::helper(bool commit) {
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  // Selects two out of three rows.
  auto table_scan = create_table_scan(_gt, ColumnID{1}, PredicateCondition::GreaterThan, 456.7f);

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);

  delete_op->execute();

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(0u),
            transaction_context->transaction_id());
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids.at(2u),
            transaction_context->transaction_id());

  auto expected_end_cid = CommitID{0u};
  if (commit) {
    transaction_context->commit();
    expected_end_cid = transaction_context->commit_id();
  } else {
    transaction_context->rollback();
    expected_end_cid = MvccData::MAX_COMMIT_ID;
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
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context();

  auto table_scan1 = create_table_scan(_gt, ColumnID{0}, PredicateCondition::Equals, "123");
  auto expected_result = create_table_scan(_gt, ColumnID{0}, PredicateCondition::NotEquals, "123");
  auto table_scan2 = create_table_scan(_gt, ColumnID{0}, PredicateCondition::LessThan, "1234");

  table_scan1->execute();
  expected_result->execute();
  table_scan2->execute();

  EXPECT_EQ(table_scan1->get_output()->chunk_count(), 1u);
  EXPECT_EQ(table_scan1->get_output()->get_chunk(ChunkID{0})->column_count(), 2u);

  auto delete_op1 = std::make_shared<Delete>(table_scan1);
  delete_op1->set_transaction_context(t1_context);

  auto delete_op2 = std::make_shared<Delete>(table_scan2);
  delete_op2->set_transaction_context(t2_context);

  delete_op1->execute();
  delete_op2->execute();

  EXPECT_FALSE(delete_op1->execute_failed());
  EXPECT_TRUE(delete_op2->execute_failed());

  // MVCC commit.
  t1_context->commit();
  t2_context->rollback();

  // Get validated table which should have only one row deleted.
  auto t_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(t_context);

  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result->get_output());
}

TEST_F(OperatorsDeleteTest, EmptyDelete) {
  auto tx_context_modification = Hyrise::get().transaction_manager.new_transaction_context();

  auto table_scan = create_table_scan(_gt, ColumnID{0}, PredicateCondition::Equals, "112233");

  table_scan->execute();

  EXPECT_EQ(table_scan->get_output()->chunk_count(), 0u);

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(tx_context_modification);

  delete_op->execute();

  EXPECT_FALSE(delete_op->execute_failed());

  // MVCC commit.
  tx_context_modification->commit();

  // Get validated table which should be the original one
  auto tx_context_verification = Hyrise::get().transaction_manager.new_transaction_context();
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(tx_context_verification);

  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), _gt->get_output());
}

TEST_F(OperatorsDeleteTest, UpdateAfterDeleteFails) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context();

  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(t1_context);

  auto validate2 = std::make_shared<Validate>(_gt);
  validate2->set_transaction_context(t2_context);

  validate1->execute();
  validate2->execute();

  auto delete_op = std::make_shared<Delete>(validate1);
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

  for (const auto value : {456.7f, 457.7f}) {
    auto context = Hyrise::get().transaction_manager.new_transaction_context();

    auto values_to_insert = load_table("resources/test_data/tbl/int_float3.tbl");
    auto table_name_for_insert = "bla";
    Hyrise::get().storage_manager.add_table(table_name_for_insert, values_to_insert);
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

    auto delete_op = std::make_shared<Delete>(table_scan1);
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

    if (value == 456.7f) {
      context->rollback();
    } else {
      context->commit();
    }

    Hyrise::get().storage_manager.drop_table(table_name_for_insert);
  }

  {
    auto context = Hyrise::get().transaction_manager.new_transaction_context();

    auto gt = std::make_shared<GetTable>(_table_name);
    gt->execute();

    auto validate1 = std::make_shared<Validate>(_gt);
    validate1->set_transaction_context(context);
    validate1->execute();

    auto expected_result = load_table("resources/test_data/tbl/int_float_deleted.tbl");

    EXPECT_TABLE_EQ_UNORDERED(validate1->get_output(), expected_result);

    context->rollback();
  }
}

// This test uses the transaction context after its already been committed on behalf of every
// read/write operator and the read only operators Validate and GetTable
TEST_F(OperatorsDeleteTest, UseTransactionContextAfterCommit) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context();

  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(t1_context);
  validate1->execute();

  auto delete_op = std::make_shared<Delete>(validate1);
  delete_op->set_transaction_context(t1_context);
  delete_op->execute();

  t1_context->commit();

  auto delete_op2 = std::make_shared<Delete>(validate1);
  delete_op->set_transaction_context(t1_context);

  EXPECT_THROW(delete_op->execute(), std::logic_error);
}

TEST_F(OperatorsDeleteTest, RunOnUnvalidatedTable) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();

  const auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::LessThan, 10000);
  table_scan->execute();

  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto delete_op1 = std::make_shared<Delete>(table_scan);
  delete_op1->set_transaction_context(t1_context);
  // This one works and deletes some rows
  delete_op1->execute();
  t1_context->commit();

  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto delete_op2 = std::make_shared<Delete>(table_scan);
  delete_op2->set_transaction_context(t2_context);
  // This one should fail because the rows should have been filtered out by a validate and should not be visible
  // to the delete operator in the first place.
  EXPECT_THROW(delete_op2->execute(), std::logic_error);
  t2_context->rollback();
}

TEST_F(OperatorsDeleteTest, PrunedInputTable) {
  // Test that the input table of Delete can reference either a stored table or a pruned version of a stored table
  // (i.e., a table containing a subset of the chunks of the stored table)

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  // Create the values_to_delete table via Chunk pruning and a Table Scan
  const auto get_table_op = std::make_shared<GetTable>("table_b", std::vector{ChunkID{1}}, std::vector<ColumnID>{});
  get_table_op->execute();

  const auto table_scan = create_table_scan(get_table_op, ColumnID{0}, PredicateCondition::LessThan, 5);
  table_scan->execute();

  const auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  transaction_context->commit();

  const auto expected_end_cid = transaction_context->commit_id();
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(0u), expected_end_cid);
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(1u), expected_end_cid);
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids.at(2u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->get_scoped_mvcc_data_lock()->end_cids.at(0u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->get_scoped_mvcc_data_lock()->end_cids.at(1u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->get_scoped_mvcc_data_lock()->end_cids.at(2u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{2})->get_scoped_mvcc_data_lock()->end_cids.at(0u), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{2})->get_scoped_mvcc_data_lock()->end_cids.at(1u), expected_end_cid);
}

}  // namespace opossum
