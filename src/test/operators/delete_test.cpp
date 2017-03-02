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
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsDeleteTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = load_table("src/test/tables/float_int.tbl", 0u);
    StorageManager::get().add_table("table_a", _table);
    _gt = std::make_shared<GetTable>("table_a");

    _gt->execute();
  }

  std::shared_ptr<GetTable> _gt;
  std::shared_ptr<Table> _table;
};

TEST_F(OperatorsDeleteTest, ExecuteAndCommit) {
  auto transaction_context = TransactionContext{1u, 1u};
  const auto cid = 1u;

  auto table_scan = std::make_shared<TableScan>(_gt, "b", ">", "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);

  delete_op->execute(&transaction_context);

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(0u), transaction_context.transaction_id());
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(2u), transaction_context.transaction_id());

  delete_op->commit(cid);

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(0u), cid);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(1u), Chunk::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(2u), cid);

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(0u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(2u), 0u);
}

TEST_F(OperatorsDeleteTest, ExecuteAndAbort) {
  auto transaction_context = TransactionContext{1u, 1u};

  auto table_scan = std::make_shared<TableScan>(_gt, "b", ">", "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);

  delete_op->execute(&transaction_context);

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(0u), transaction_context.transaction_id());
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(2u), transaction_context.transaction_id());

  delete_op->abort();

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(0u), Chunk::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(1u), Chunk::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().end_cids.at(2u), Chunk::MAX_COMMIT_ID);

  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(0u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u).mvcc_columns().tids.at(2u), 0u);
}

TEST_F(OperatorsDeleteTest, DetectDirtyWrite) {
  auto t1_context = TransactionManager::get().new_transaction_context();
  auto t2_context = TransactionManager::get().new_transaction_context();

  auto table_scan1 = std::make_shared<TableScan>(_gt, "a", "=", "123");
  auto expected_result = std::make_shared<TableScan>(_gt, "a", "!=", "123");
  auto table_scan2 = std::make_shared<TableScan>(_gt, "a", "<=", "1234");

  table_scan1->execute();
  expected_result->execute();
  table_scan2->execute();

  EXPECT_EQ(table_scan1->get_output()->chunk_count(), 1u);
  EXPECT_EQ(table_scan1->get_output()->get_chunk(0).col_count(), 2u);

  auto delete_op1 = std::make_shared<Delete>(table_scan1);
  auto delete_op2 = std::make_shared<Delete>(table_scan2);

  delete_op1->execute(t1_context.get());
  delete_op2->execute(t2_context.get());

  EXPECT_FALSE(delete_op1->execute_failed());
  EXPECT_TRUE(delete_op2->execute_failed());

  // MVCC commit.
  TransactionManager::get().prepare_commit(*t1_context);

  delete_op1->commit(t1_context->commit_id());

  TransactionManager::get().commit(*t1_context);

  // Get validated table which should have only one row deleted.
  auto t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(_gt);
  validate->execute(t_context.get());

  EXPECT_TABLE_EQ(validate->get_output(), expected_result->get_output());
}

}  // namespace opossum
