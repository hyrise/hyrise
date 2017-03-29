#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/commit_records.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsUpdateTest : public BaseTest {
 protected:
  void SetUp() override {}
  void helper(std::shared_ptr<GetTable> table_to_update, std::shared_ptr<GetTable> update_values,
              std::shared_ptr<Table> expected_result);
};

void OperatorsUpdateTest::helper(std::shared_ptr<GetTable> table_to_update, std::shared_ptr<GetTable> update_values,
                                 std::shared_ptr<Table> expected_result) {
  auto t_context = TransactionManager::get().new_transaction_context();

  // Make input left actually referenced. Projection does NOT generate ReferenceColumns.
  auto ref_table = std::make_shared<TableScan>(table_to_update, "a", ">", 0);
  ref_table->set_transaction_context(t_context);
  ref_table->execute();

  std::vector<std::string> column_filter_left = {"a"};
  std::vector<std::string> column_filter_right = {"b"};

  auto projection1 = std::make_shared<Projection>(ref_table, column_filter_left);
  auto projection2 = std::make_shared<Projection>(update_values, column_filter_right);
  projection1->set_transaction_context(t_context);
  projection2->set_transaction_context(t_context);
  projection1->execute();
  projection2->execute();

  auto update = std::make_shared<Update>("updateTestTable", projection1, projection2);
  update->set_transaction_context(t_context);
  update->execute();

  // MVCC commit.
  TransactionManager::get().prepare_commit(*t_context);

  auto commit_op = std::make_shared<CommitRecords>();
  commit_op->set_transaction_context(t_context);
  commit_op->execute();

  TransactionManager::get().commit(*t_context);

  // Get validated table which should have the same row twice.
  t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(table_to_update);
  validate->set_transaction_context(t_context);
  validate->execute();

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}

TEST_F(OperatorsUpdateTest, SelfUpdate) {
  auto t = load_table("src/test/tables/int_int.tbl", 0u);
  StorageManager::get().add_table("updateTestTable", t);

  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt, expected_result);
}

TEST_F(OperatorsUpdateTest, NormalUpdate) {
  auto t = load_table("src/test/tables/int_int.tbl", 0u);
  StorageManager::get().add_table("updateTestTable", t);

  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", 0u);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}

TEST_F(OperatorsUpdateTest, MultipleChunksLeft) {
  auto t = load_table("src/test/tables/int_int.tbl", 2u);
  StorageManager::get().add_table("updateTestTable", t);

  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", 0u);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}

TEST_F(OperatorsUpdateTest, MultipleChunksRight) {
  auto t = load_table("src/test/tables/int_int.tbl", 0u);
  StorageManager::get().add_table("updateTestTable", t);

  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", 2u);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}

TEST_F(OperatorsUpdateTest, MultipleChunks) {
  auto t = load_table("src/test/tables/int_int.tbl", 2u);
  StorageManager::get().add_table("updateTestTable", t);

  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", 1u);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}
}  // namespace opossum
