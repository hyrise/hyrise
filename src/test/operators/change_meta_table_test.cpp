#include <memory>
#include <string>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "operators/change_meta_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "utils/meta_tables/meta_mock_table.hpp"

namespace opossum {

class ChangeMetaTableTest : public BaseTest {
 protected:
  void SetUp() {
    Hyrise::reset();

    auto column_definitions = MetaMockTable().column_definitions();
    auto mock_table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    mock_table->append({pmr_string{"foo"}});
    input_left = std::make_shared<TableWrapper>(std::move(mock_table));

    auto other_mock_table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    other_mock_table->append({pmr_string{"bar"}});
    input_right = std::make_shared<TableWrapper>(std::move(other_mock_table));

    input_left->execute();
    input_right->execute();

    meta_mock_table = std::make_shared<MetaMockTable>();
    Hyrise::get().meta_table_manager._add(meta_mock_table);

    context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  }

  void TearDown() { Hyrise::reset(); }

  std::shared_ptr<AbstractOperator> input_left;
  std::shared_ptr<AbstractOperator> input_right;
  std::shared_ptr<MetaMockTable> meta_mock_table;
  std::shared_ptr<TransactionContext> context;
};

TEST_F(ChangeMetaTableTest, Insert) {
  auto change_meta_table =
      std::make_shared<ChangeMetaTable>("meta_mock", MetaTableChangeType::Insert, input_left, input_right);

  change_meta_table->set_transaction_context(context);
  change_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->insert_calls(), 1);
  EXPECT_EQ(meta_mock_table->insert_values(), input_right->get_output()->get_row(0));
}

TEST_F(ChangeMetaTableTest, Delete) {
  auto change_meta_table =
      std::make_shared<ChangeMetaTable>("meta_mock", MetaTableChangeType::Delete, input_left, input_right);

  change_meta_table->set_transaction_context(context);
  change_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->remove_calls(), 1);
  EXPECT_EQ(meta_mock_table->remove_values(), input_left->get_output()->get_row(0));
}

TEST_F(ChangeMetaTableTest, Update) {
  auto change_meta_table =
      std::make_shared<ChangeMetaTable>("meta_mock", MetaTableChangeType::Update, input_left, input_right);

  change_meta_table->set_transaction_context(context);
  change_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->update_calls(), 1);
  EXPECT_EQ(meta_mock_table->update_selected_values(), input_left->get_output()->get_row(0));
  EXPECT_EQ(meta_mock_table->update_updated_values(), input_right->get_output()->get_row(0));
}

TEST_F(ChangeMetaTableTest, OnlyAllowsAutoCommit) {
  auto change_meta_table =
      std::make_shared<ChangeMetaTable>("meta_mock", MetaTableChangeType::Insert, input_left, input_right);

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  change_meta_table->set_transaction_context(transaction_context);

  EXPECT_THROW(change_meta_table->execute(), std::exception);

  transaction_context->rollback();
}

}  // namespace opossum
