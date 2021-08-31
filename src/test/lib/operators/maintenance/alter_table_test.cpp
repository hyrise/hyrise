
#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/alter_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "tasks/chunk_compression_task.hpp"
#include "logical_query_plan/drop_column_action.hpp"


namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

std::shared_ptr<DropColumnAction> _create_drop_column_action(char* column_name, bool if_exists) {
  auto alter_table_action = hsql::DropColumnAction(column_name);
  auto drop_column_action = std::make_shared<DropColumnAction>(alter_table_action);
  drop_column_action->if_exists = if_exists;
  return drop_column_action;
}

class AlterTableTest : public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table(test_table_name, test_table);
  }

  std::shared_ptr<std::vector<ColumnID>> column_ids = std::make_shared<std::vector<ColumnID>>();
  std::shared_ptr<Table> test_table;
  std::string test_table_name = "TestTable";
};

TEST_F(AlterTableTest, NameAndDescriptionDropColumn) {
  char column_name[] = {'b', '\0'};
  const auto action = _create_drop_column_action(column_name, false);
  auto alter_drop_column = std::make_shared<AlterTable>(test_table_name, action);

  EXPECT_EQ(alter_drop_column->name(), "AlterTable");
  EXPECT_EQ(alter_drop_column->description(DescriptionMode::SingleLine), "AlterTable 'TestTable' DropColumn 'b'");
}

TEST_F(AlterTableTest, Execute) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  char column_name[] = {'b', '\0'};
  const auto action = _create_drop_column_action(column_name, false);
  auto alter_drop_column = std::make_shared<AlterTable>(test_table_name, action);

  alter_drop_column->set_transaction_context(context);

  alter_drop_column->execute();
  context->commit();

  EXPECT_EQ(test_table->column_count(), 1u);
  EXPECT_EQ(test_table->column_id_by_name("a"), 0u);
}

TEST_F(AlterTableTest, NoSuchColumn) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  char column_name[] = {'c', '\0'};
  const auto action = _create_drop_column_action(column_name, false);
  auto alter_drop_column = std::make_shared<AlterTable>(test_table_name, action);
  alter_drop_column->set_transaction_context(context);

  EXPECT_THROW(alter_drop_column->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(AlterTableTest, NoSuchTable) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  char column_name[] = {'b', '\0'};
  const auto action = _create_drop_column_action(column_name, false);
  auto alter_drop_column = std::make_shared<AlterTable>("NotExistingTable", action);
  alter_drop_column->set_transaction_context(context);

  EXPECT_THROW(alter_drop_column->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(AlterTableTest, NoSuchColumnWithNotExists) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  char column_name[] = {'c', '\0'};
  const auto action = _create_drop_column_action(column_name, true);
  auto alter_drop_column = std::make_shared<AlterTable>(test_table_name, action);
  alter_drop_column->set_transaction_context(context);

  EXPECT_NO_THROW(alter_drop_column->execute());
  context->commit();
}

}  // namespace opossum
