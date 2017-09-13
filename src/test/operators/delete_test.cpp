#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/delete.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsDeleteTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_name = "table_a";
    _table = load_table("src/test/tables/float_int.tbl", 0u);
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
  auto transaction_context = std::make_shared<TransactionContext>(1u, 1u);
  const auto cid = 1u;

  // Selects two out of three rows.
  auto table_scan = std::make_shared<TableScan>(_gt, ColumnID{0}, ScanType::OpGreaterThan, "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(_table_name, table_scan);
  delete_op->set_transaction_context(transaction_context);

  delete_op->execute();

  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(0u), transaction_context->transaction_id());
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(2u), transaction_context->transaction_id());

  // Table has three rows initially.
  EXPECT_EQ(_table->approx_valid_row_count(), 3u);

  auto expected_end_cid = cid;
  if (commit) {
    delete_op->commit(cid);

    // Delete successful, one row left.
    EXPECT_EQ(_table->approx_valid_row_count(), 1u);
  } else {
    delete_op->rollback_records();
    expected_end_cid = Chunk::MAX_COMMIT_ID;

    // Delete rolled back, three rows left.
    EXPECT_EQ(_table->approx_valid_row_count(), 3u);
  }

  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->end_cids.at(0u), expected_end_cid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->end_cids.at(1u), Chunk::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->end_cids.at(2u), expected_end_cid);

  auto expected_tid = commit ? transaction_context->transaction_id() : 0u;

  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(0u), expected_tid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0}).mvcc_columns()->tids.at(2u), expected_tid);
}

TEST_F(OperatorsDeleteTest, ExecuteAndCommit) { helper(true); }

TEST_F(OperatorsDeleteTest, ExecuteAndAbort) { helper(false); }

TEST_F(OperatorsDeleteTest, DetectDirtyWrite) {
  auto t1_context = TransactionManager::get().new_transaction_context();
  auto t2_context = TransactionManager::get().new_transaction_context();

  auto table_scan1 = std::make_shared<TableScan>(_gt, ColumnID{1}, ScanType::OpEquals, "123");
  auto expected_result = std::make_shared<TableScan>(_gt, ColumnID{1}, ScanType::OpNotEquals, "123");
  auto table_scan2 = std::make_shared<TableScan>(_gt, ColumnID{1}, ScanType::OpLessThan, "1234");

  table_scan1->execute();
  expected_result->execute();
  table_scan2->execute();

  EXPECT_EQ(table_scan1->get_output()->chunk_count(), 1u);
  EXPECT_EQ(table_scan1->get_output()->get_chunk(ChunkID{0}).col_count(), 2u);

  auto delete_op1 = std::make_shared<Delete>(_table_name, table_scan1);
  delete_op1->set_transaction_context(t1_context);

  auto delete_op2 = std::make_shared<Delete>(_table_name, table_scan2);
  delete_op2->set_transaction_context(t2_context);

  delete_op1->execute();
  delete_op2->execute();

  EXPECT_FALSE(delete_op1->execute_failed());
  EXPECT_TRUE(delete_op2->execute_failed());

  // MVCC commit.
  TransactionManager::get().prepare_commit(*t1_context);

  delete_op1->commit(t1_context->commit_id());

  TransactionManager::get().commit(*t1_context);

  // Get validated table which should have only one row deleted.
  auto t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(t_context);

  validate->execute();

  EXPECT_TABLE_EQ(validate->get_output(), expected_result->get_output());
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
  TransactionManager::get().prepare_commit(*t1_context);
  delete_op->commit(t1_context->commit_id());
  EXPECT_FALSE(delete_op->execute_failed());

  // this update tries to update the values that have been deleted in another transaction and should fail.
  auto update_op = std::make_shared<Update>(_table_name, validate2, validate2);
  update_op->set_transaction_context(t2_context);
  update_op->execute();
  EXPECT_TRUE(update_op->execute_failed());
}

}  // namespace opossum
