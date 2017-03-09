#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/abort.hpp"
#include "../../lib/operators/delete.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {
class OperatorsAbortTest : public BaseTest {
 protected:
  void SetUp() override {
    auto t = load_table("src/test/tables/int_int.tbl", 0u);
    table_name = "aNiceTestTable";
    StorageManager::get().add_table(table_name, t);
  }

  std::shared_ptr<Table> _test_table;
  std::string table_name;
};

TEST_F(OperatorsAbortTest, AbortDelete) {
  auto expected_result = load_table("src/test/tables/int_int.tbl", 1);

  auto t_context = TransactionManager::get().new_transaction_context();

  // Get and scan table to make columns referenced.
  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();
  auto table_scan = std::make_shared<TableScan>(gt, "a", ">=", "0");
  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_name, table_scan);
  delete_op->execute(t_context.get());

  auto abort_op = std::make_shared<Abort>();
  abort_op->execute(t_context.get());

  // Get validated table which should not have any deleted rows.
  t_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(gt);
  validate->execute(t_context.get());

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}

}  // namespace opossum
