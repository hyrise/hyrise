#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsUpdateTest : public BaseTest {
 protected:
  void SetUp() override {
    auto t = load_table("src/test/tables/int_int.tbl", Chunk::MAX_SIZE);
    // Update operator works on the StorageManager
    StorageManager::get().add_table(_table_name, t);
  }

  void TearDown() override { StorageManager::reset(); }

  void helper(std::shared_ptr<GetTable> table_to_update, std::shared_ptr<GetTable> update_values,
              std::shared_ptr<Table> expected_result);

  std::string _table_name{"updateTestTable"};
};

void OperatorsUpdateTest::helper(std::shared_ptr<GetTable> table_to_update, std::shared_ptr<GetTable> update_values,
                                 std::shared_ptr<Table> expected_result) {
  auto t_context = TransactionManager::get().new_transaction_context();

  // Make input left actually referenced. Projection does NOT generate ReferenceSegments.
  auto ref_table = std::make_shared<TableScan>(table_to_update,
                                               OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 0});
  ref_table->set_transaction_context(t_context);
  ref_table->execute();

  // Save the original number of rows as well as the number of rows that will be updated.
  auto original_row_count = ref_table->get_output()->row_count();
  auto updated_rows_count = update_values->get_output()->row_count();

  auto projection1 = std::make_shared<Projection>(
      ref_table, expression_vector(PQPColumnExpression::from_table(*ref_table->get_output(), "a")));
  auto projection2 = std::make_shared<Projection>(
      ref_table, expression_vector(PQPColumnExpression::from_table(*ref_table->get_output(), "b")));
  projection1->set_transaction_context(t_context);
  projection2->set_transaction_context(t_context);
  projection1->execute();
  projection2->execute();

  auto update = std::make_shared<Update>("updateTestTable", projection1, projection2);
  update->set_transaction_context(t_context);
  update->execute();

  // MVCC commit.
  t_context->commit();

  // Get validated table which should have the same row twice.
  t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(table_to_update);
  validate->set_transaction_context(t_context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);

  // The new validated table should have the same number of (valid) rows as before.
  EXPECT_EQ(validate->get_output()->row_count(), original_row_count);

  // Refresh the table that was updated. It should have the same number of valid rows (approximated) as before.
  // Approximation should be exact here because we do not have to deal with parallelism issues in tests.
  auto updated_table = std::make_shared<GetTable>("updateTestTable");
  updated_table->execute();
  ASSERT_NE(updated_table->get_output()->table_statistics(), nullptr);
  EXPECT_EQ(updated_table->get_output()->table_statistics()->row_count(), 3u);
  EXPECT_EQ(updated_table->get_output()->table_statistics()->approx_valid_row_count(), 0u);
  EXPECT_EQ(updated_table->get_output()->row_count(), original_row_count + updated_rows_count);
}

TEST_F(OperatorsUpdateTest, SelfUpdate) {
  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt, expected_result);
}

TEST_F(OperatorsUpdateTest, NormalUpdate) {
  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}

TEST_F(OperatorsUpdateTest, MultipleChunksLeft) {
  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", Chunk::MAX_SIZE);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}

TEST_F(OperatorsUpdateTest, MultipleChunksRight) {
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
  auto gt = std::make_shared<GetTable>("updateTestTable");
  gt->execute();

  auto t2 = load_table("src/test/tables/int_int.tbl", 1u);
  StorageManager::get().add_table("updateTestTable2", t2);

  auto gt2 = std::make_shared<GetTable>("updateTestTable2");
  gt2->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);
  helper(gt, gt2, expected_result);
}
TEST_F(OperatorsUpdateTest, MissingChunks) {
  auto t_context = TransactionManager::get().new_transaction_context();

  auto gt = std::make_shared<GetTable>(_table_name);
  gt->execute();

  // table scan will leave out first two chunks
  auto table_scan1 =
      std::make_shared<TableScan>(gt, OperatorScanPredicate{ColumnID{0}, PredicateCondition::Equals, "12345"});
  table_scan1->set_transaction_context(t_context);
  table_scan1->execute();

  auto updated_rows = std::make_shared<Projection>(table_scan1, expression_vector(1, 1));
  updated_rows->set_transaction_context(t_context);
  updated_rows->execute();

  auto update = std::make_shared<Update>(_table_name, table_scan1, updated_rows);
  update->set_transaction_context(t_context);
  // execute will fail, if not checked for leading empty chunks
  update->execute();

  // MVCC commit.
  t_context->commit();
}
}  // namespace opossum
