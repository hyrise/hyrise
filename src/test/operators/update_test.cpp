#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class UpdateTest : public BaseTest {
 protected:
  void SetUp() override {
    t = load_table("src/test/tables/int_int.tbl", 0u);
    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();

    t2 = load_table("src/test/tables/int_int.tbl", 0u);
    StorageManager::get().add_table(table_name2, t2);

    gt2 = std::make_shared<GetTable>(table_name2);
    gt2->execute();
  }

  TransactionManager& manager() { return TransactionManager::get(); }

  std::ostringstream output;

  std::string table_name = "updateTestTable";
  std::string table_name2 = "updateTestTable2";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable> gt;
  std::shared_ptr<Table> t = nullptr;

  std::shared_ptr<GetTable> gt2;
  std::shared_ptr<Table> t2 = nullptr;
};

TEST_F(UpdateTest, SelfUpdate) {
  auto t_context = manager().new_transaction_context();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);

  std::vector<std::string> column_filter_left = {"a"};
  std::vector<std::string> column_filter_right = {"b"};

  // make input left actually referenced. Projection does NOT generate ReferenceColumns
  // TODO(all): rethink update which handles non-refcols.
  auto ref_table = std::make_shared<TableScan>(gt, "a", ">", 0);
  ref_table->execute(t_context.get());

  auto projection1 = std::make_shared<Projection>(ref_table, column_filter_left);
  auto projection2 = std::make_shared<Projection>(gt, column_filter_right);
  projection1->execute(t_context.get());
  projection2->execute(t_context.get());

  auto update = std::make_shared<Update>(projection1, projection2);
  update->execute(t_context.get());

  // MVCC commit.
  manager().prepare_commit(*t_context);
  update->commit(t_context->commit_id());
  manager().commit(*t_context);

  // Get validated table which should have the same row twice.
  t_context = manager().new_transaction_context();
  auto validate = std::make_shared<Validate>(gt);
  validate->execute(t_context.get());

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}

TEST_F(UpdateTest, NormalUpdate) {
  auto t_context = manager().new_transaction_context();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_same.tbl", 1);

  std::vector<std::string> column_filter_left = {"a"};
  std::vector<std::string> column_filter_right = {"b"};

  // make input left actually referenced. Projection does NOT generate ReferenceColumns
  // TODO(all): rethink update which handles non-refcols.
  auto ref_table = std::make_shared<TableScan>(gt, "a", ">", 0);
  ref_table->execute(t_context.get());

  auto projection1 = std::make_shared<Projection>(ref_table, column_filter_left);
  auto projection2 = std::make_shared<Projection>(gt2, column_filter_right);
  projection1->execute(t_context.get());
  projection2->execute(t_context.get());

  auto update = std::make_shared<Update>(projection1, projection2);
  update->execute(t_context.get());

  // MVCC commit.
  manager().prepare_commit(*t_context);
  update->commit(t_context->commit_id());
  manager().commit(*t_context);

  // Get validated table which should have the same row twice.
  t_context = manager().new_transaction_context();
  auto validate = std::make_shared<Validate>(gt);
  validate->execute(t_context.get());

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}
}  // namespace opossum
