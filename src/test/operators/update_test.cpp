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
  auto ref_table = std::make_shared<TableScan>(table_to_update, ColumnID{0}, ScanType::OpGreaterThan, 0);
  ref_table->set_transaction_context(t_context);
  ref_table->execute();

  // Save the original number of rows as well as the number of rows that will be updated.
  auto original_row_count = ref_table->get_output()->row_count();
  auto updated_rows_count = update_values->get_output()->row_count();

  auto projection1 =
      std::make_shared<Projection>(ref_table, Projection::ColumnExpressions({Expression::create_column(ColumnID{0})}));
  auto projection2 =
      std::make_shared<Projection>(ref_table, Projection::ColumnExpressions({Expression::create_column(ColumnID{1})}));
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

  // The new validated table should have the same number of (valid) rows as before.
  EXPECT_EQ(validate->get_output()->row_count(), original_row_count);

  // Refresh the table that was updated. It should have the same number of valid rows (approximated) as before.
  // Approximation should be exact here because we do not have ti deal with parallelism issues in tests.
  auto updated_table = std::make_shared<GetTable>("updateTestTable");
  updated_table->execute();
  EXPECT_EQ(updated_table->get_output()->approx_valid_row_count(), original_row_count);

  // The total row count (valid + invalid) should have increased by the number of rows that were updated.
  EXPECT_EQ(updated_table->get_output()->row_count(), original_row_count + updated_rows_count);
}

TEST_F(OperatorsUpdateTest, SelfUpdate) {
  auto t = load_table("src/test/tables/int_int.tbl", 0u);
  // Update operator works on the StorageManager
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
TEST_F(OperatorsUpdateTest, EmptyChunks) {
  auto t = load_table("src/test/tables/int.tbl", 1u);
  std::string table_name = "updateTestTable";
  StorageManager::get().add_table(table_name, t);

  auto t_context = TransactionManager::get().new_transaction_context();

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  // table scan will produce two leading empty chunks
  auto table_scan1 = std::make_shared<TableScan>(gt, ColumnID{0}, ScanType::OpEquals, "12345");
  table_scan1->set_transaction_context(t_context);
  table_scan1->execute();

  Projection::ColumnExpressions column_expressions{Expression::create_literal(1, {"a"})};
  auto updated_rows = std::make_shared<Projection>(table_scan1, column_expressions);
  updated_rows->set_transaction_context(t_context);
  updated_rows->execute();

  auto update = std::make_shared<Update>(table_name, table_scan1, updated_rows);
  update->set_transaction_context(t_context);
  // execute will fail, if not checked for leading empty chunks
  update->execute();

  // MVCC commit.
  TransactionManager::get().prepare_commit(*t_context);

  auto commit_op = std::make_shared<CommitRecords>();
  commit_op->set_transaction_context(t_context);
  commit_op->execute();

  TransactionManager::get().commit(*t_context);
}
}  // namespace opossum
