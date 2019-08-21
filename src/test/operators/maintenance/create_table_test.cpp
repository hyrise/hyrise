#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

class CreateTableTest : public BaseTest {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);

    dummy_table_wrapper = std::make_shared<TableWrapper>(Table::create_dummy_table(column_definitions));
    dummy_table_wrapper->execute();

    create_table = std::make_shared<CreateTable>("t", false, dummy_table_wrapper);
  }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<TableWrapper> dummy_table_wrapper;
  std::shared_ptr<CreateTable> create_table;
};

TEST_F(CreateTableTest, NameAndDescription) {
  EXPECT_EQ(create_table->name(), "Create Table");
  EXPECT_EQ(create_table->description(DescriptionMode::SingleLine),
            "Create Table 't' ('a' int NOT NULL, 'b' float NULL)");
  EXPECT_EQ(create_table->description(DescriptionMode::MultiLine),
            "Create Table 't' ('a' int NOT NULL\n'b' float NULL)");
}

TEST_F(CreateTableTest, Execute) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context();
  create_table->set_transaction_context(context);

  create_table->execute();
  context->commit();

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("t"));

  const auto table = Hyrise::get().storage_manager.get_table("t");

  EXPECT_EQ(table->row_count(), 0);
  EXPECT_EQ(table->column_definitions(), column_definitions);
}

TEST_F(CreateTableTest, TableAlreadyExists) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context();
  create_table->set_transaction_context(context);

  create_table->execute();  // Table name "t" is taken now
  context->commit();

  const auto create_different_table = std::make_shared<CreateTable>("t2", false, dummy_table_wrapper);
  const auto create_same_table = std::make_shared<CreateTable>("t", false, dummy_table_wrapper);

  const auto context_2 = Hyrise::get().transaction_manager.new_transaction_context();
  const auto context_3 = Hyrise::get().transaction_manager.new_transaction_context();
  create_different_table->set_transaction_context(context_2);
  create_same_table->set_transaction_context(context_3);

  EXPECT_NO_THROW(create_different_table->execute());
  context_2->commit();

  EXPECT_THROW(create_same_table->execute(), std::logic_error);
  context_3->rollback();
}

TEST_F(CreateTableTest, ExecuteWithIfNotExists) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context();
  const auto ct_if_not_exists_1 = std::make_shared<CreateTable>("t", true, dummy_table_wrapper);
  ct_if_not_exists_1->set_transaction_context(context);

  ct_if_not_exists_1->execute();
  context->commit();

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("t"));

  const auto table = Hyrise::get().storage_manager.get_table("t");

  EXPECT_EQ(table->row_count(), 0);
  EXPECT_EQ(table->column_definitions(), column_definitions);

  const auto context_2 = Hyrise::get().transaction_manager.new_transaction_context();
  const auto ct_if_not_exists_2 = std::make_shared<CreateTable>("t", true, dummy_table_wrapper);
  ct_if_not_exists_2->set_transaction_context(context_2);

  EXPECT_NO_THROW(ct_if_not_exists_2->execute());
  context_2->commit();
}

TEST_F(CreateTableTest, CreateTableAsSelect) {
  const auto table = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table("test", table);
  const auto context = Hyrise::get().transaction_manager.new_transaction_context();

  const auto get_table = std::make_shared<GetTable>("test");
  get_table->set_transaction_context(context);
  get_table->execute();

  const auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(context);
  validate->execute();

  const auto create_table_as = std::make_shared<CreateTable>("test_2", false, validate);
  create_table_as->set_transaction_context(context);
  EXPECT_NO_THROW(create_table_as->execute());
  context->commit();

  const auto created_table = Hyrise::get().storage_manager.get_table("test_2");
  EXPECT_TABLE_EQ_ORDERED(created_table, table);

  Hyrise::get().storage_manager.drop_table("test");

  EXPECT_TABLE_EQ_ORDERED(created_table, table);
}

TEST_F(CreateTableTest, CreateTableAsSelectWithProjection) {
  const auto table = load_table("resources/test_data/tbl/int_float.tbl");
  Hyrise::get().storage_manager.add_table("test", table);
  const auto context = Hyrise::get().transaction_manager.new_transaction_context();

  const auto get_table = std::make_shared<GetTable>("test");
  get_table->set_transaction_context(context);
  get_table->execute();

  const auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(context);
  validate->execute();

  const std::shared_ptr<AbstractExpression> expr =
      add_(PQPColumnExpression::from_table(*table, "a"), PQPColumnExpression::from_table(*table, "b"));
  const auto projection = std::make_shared<Projection>(validate, expression_vector(expr));
  projection->set_transaction_context(context);
  projection->execute();

  const auto create_table_as = std::make_shared<CreateTable>("test_2", false, projection);
  create_table_as->set_transaction_context(context);
  EXPECT_NO_THROW(create_table_as->execute());

  context->commit();

  const auto created_table = Hyrise::get().storage_manager.get_table("test_2");

  EXPECT_TABLE_EQ_ORDERED(created_table, load_table("resources/test_data/tbl/projection/int_float_add.tbl"));
}

TEST_F(CreateTableTest, CreateTableWithDifferentTransactionContexts) {
  const auto table = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table("test", table);

  const auto context_1 = Hyrise::get().transaction_manager.new_transaction_context();
  const auto context_2 = Hyrise::get().transaction_manager.new_transaction_context();
  const auto context_3 = Hyrise::get().transaction_manager.new_transaction_context();

  // Create table 1 with second context
  const auto get_table_1 = std::make_shared<GetTable>("test");
  get_table_1->set_transaction_context(context_2);
  get_table_1->execute();

  const auto validate_1 = std::make_shared<Validate>(get_table_1);
  validate_1->set_transaction_context(context_2);
  validate_1->execute();

  const auto create_table_as_1 = std::make_shared<CreateTable>("test_2", false, validate_1);
  create_table_as_1->set_transaction_context(context_2);
  EXPECT_NO_THROW(create_table_as_1->execute());

  // Create table 2 with first context, which should not see the rows of table 1
  const auto get_table_2 = std::make_shared<GetTable>("test_2");
  get_table_2->set_transaction_context(context_1);
  get_table_2->execute();

  const auto validate_2 = std::make_shared<Validate>(get_table_2);
  validate_2->set_transaction_context(context_1);
  validate_2->execute();

  const auto create_table_as_2 = std::make_shared<CreateTable>("test_3", false, validate_2);
  create_table_as_2->set_transaction_context(context_1);
  EXPECT_NO_THROW(create_table_as_2->execute());

  context_1->commit();

  const auto table_3 = Hyrise::get().storage_manager.get_table("test_3");
  EXPECT_EQ(table_3->row_count(), 0);

  context_2->rollback();

  const auto get_table_3 = std::make_shared<GetTable>("test_2");
  get_table_3->set_transaction_context(context_3);
  get_table_3->execute();

  const auto validate_3 = std::make_shared<Validate>(get_table_3);
  validate_3->set_transaction_context(context_3);
  validate_3->execute();
  context_3->commit();

  EXPECT_EQ(validate_3->get_output()->row_count(), 0);
}

}  // namespace opossum
