
#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/alter_drop_column.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "tasks/chunk_compression_task.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

class AlterTableColumnTest : public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table("TestTable", test_table);
    alter_drop_column = std::make_shared<AlterDropColumn>("TestTable", "b", false);
    alter_drop_column_if_exists = std::make_shared<AlterDropColumn>("TestTable", "b", true);
  }

  std::shared_ptr<std::vector<ColumnID>> column_ids = std::make_shared<std::vector<ColumnID>>();
  std::shared_ptr<Table> test_table;
  std::shared_ptr<AlterDropColumn> alter_drop_column;
  std::shared_ptr<AlterDropColumn> alter_drop_column_if_exists;
};

TEST_F(AlterTableColumnTest, NameAndDescription) {
  EXPECT_EQ(alter_drop_column->name(), "AlterDropColumn");
  EXPECT_EQ(alter_drop_column->description(DescriptionMode::SingleLine), "AlterDropColumn 'TestTable'('b')");
}

TEST_F(AlterTableColumnTest, Execute) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  alter_drop_column->set_transaction_context(context);

  alter_drop_column->execute();
  context->commit();

  EXPECT_EQ(test_table->column_count(), 1u);
  EXPECT_EQ(test_table->column_id_by_name("a"), 0u);
}

TEST_F(AlterTableColumnTest, NoSuchColumn) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  alter_drop_column = std::make_shared<AlterDropColumn>("TestTable", "c", false);
  alter_drop_column->set_transaction_context(context);

  EXPECT_THROW(alter_drop_column->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(AlterTableColumnTest, NoSuchTable) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  alter_drop_column = std::make_shared<AlterDropColumn>("NotExistingTable", "b", false);
  alter_drop_column->set_transaction_context(context);

  EXPECT_THROW(alter_drop_column->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(AlterTableColumnTest, NoSuchColumnWithNotExists) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  alter_drop_column = std::make_shared<AlterDropColumn>("TestTable", "c", true);
  alter_drop_column->set_transaction_context(context);

  EXPECT_NO_THROW(alter_drop_column->execute());
  context->commit();
}

}  // namespace opossum
