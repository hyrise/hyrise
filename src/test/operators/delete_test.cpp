#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
#include "../../lib/operators/delete.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class DeleteTest : public BaseTest {
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

TEST_F(DeleteTest, ExecuteAndCommit) {
  const auto transaction_context = TransactionContext{1u, 1u};
  const auto cid = 1u;
  const auto max_cid = std::numeric_limits<uint32_t>::max();

  auto table_scan = std::make_shared<TableScan>(_gt, "b", ">", "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);

  delete_op->execute(&transaction_context);

  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(0u), transaction_context.tid());
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(2u), transaction_context.tid());

  delete_op->commit(cid);

  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(0u), cid);
  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(1u), max_cid);
  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(2u), cid);

  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(0u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(2u), 0u);
}

TEST_F(DeleteTest, ExecuteAndAbort) {
  const auto transaction_context = TransactionContext{1u, 1u};
  const auto max_cid = std::numeric_limits<uint32_t>::max();

  auto table_scan = std::make_shared<TableScan>(_gt, "b", ">", "456.7");

  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);

  delete_op->execute(&transaction_context);

  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(0u), transaction_context.tid());
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(2u), transaction_context.tid());

  delete_op->abort();

  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(0u), max_cid);
  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(1u), max_cid);
  EXPECT_EQ(_table->get_chunk(0u)._end_CIDs.at(2u), max_cid);

  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(0u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(1u), 0u);
  EXPECT_EQ(_table->get_chunk(0u)._TIDs.at(2u), 0u);
}

}  // namespace opossum
