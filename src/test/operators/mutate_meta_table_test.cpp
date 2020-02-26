#include <memory>
#include <string>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "operators/mutate_meta_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "utils/meta_tables/meta_mock_table.hpp"

namespace opossum {

class MutateMetaTableTest : public BaseTest {
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
    Hyrise::get().meta_table_manager.add(meta_mock_table);

    context = Hyrise::get().transaction_manager.new_transaction_context();
  }

  void TearDown() { Hyrise::reset(); }

  std::shared_ptr<AbstractOperator> input_left;
  std::shared_ptr<AbstractOperator> input_right;
  std::shared_ptr<MetaMockTable> meta_mock_table;
  std::shared_ptr<TransactionContext> context;
};

TEST_F(MutateMetaTableTest, Insert) {
  auto mutate_meta_table =
      std::make_shared<MutateMetaTable>("meta_mock", MetaTableMutation::Insert, input_left, input_right);

  mutate_meta_table->set_transaction_context(context);
  mutate_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->insert_calls(), 1);
  EXPECT_EQ(meta_mock_table->insert_values(), input_right->get_output()->get_row(0));
}

TEST_F(MutateMetaTableTest, Delete) {
  auto mutate_meta_table =
      std::make_shared<MutateMetaTable>("meta_mock", MetaTableMutation::Delete, input_left, input_right);

  mutate_meta_table->set_transaction_context(context);
  mutate_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->remove_calls(), 1);
  EXPECT_EQ(meta_mock_table->remove_values(), input_left->get_output()->get_row(0));
}

TEST_F(MutateMetaTableTest, Update) {
  auto mutate_meta_table =
      std::make_shared<MutateMetaTable>("meta_mock", MetaTableMutation::Update, input_left, input_right);

  mutate_meta_table->set_transaction_context(context);
  mutate_meta_table->execute();

  context->commit();

  EXPECT_EQ(meta_mock_table->update_calls(), 1);
  EXPECT_EQ(meta_mock_table->update_selected_values(), input_left->get_output()->get_row(0));
  EXPECT_EQ(meta_mock_table->update_updated_values(), input_right->get_output()->get_row(0));
}

}  // namespace opossum
