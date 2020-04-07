#include <memory>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"

namespace opossum {

class ConstraintsTest : public BaseTest {
 protected:
  void SetUp() override {
    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, false);
      column_definitions.emplace_back("column2", DataType::Int, false);
      column_definitions.emplace_back("column3", DataType::Int, false);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table", table);

      table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table_nullable", table);

      table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
    }
  }
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("table");
  auto table_nullable = sm.get_table("table_nullable");

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{5}}, IsPrimaryKey::No), std::logic_error);

  // Invalid because the constraint contains duplicated columns.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{1}, ColumnID{1}}, IsPrimaryKey::No), std::logic_error);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({ColumnID{1}}, IsPrimaryKey::Yes), std::logic_error);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({ColumnID{0}, ColumnID{1}}, IsPrimaryKey::Yes),
               std::logic_error);

  table->add_soft_unique_constraint({ColumnID{2}}, IsPrimaryKey::Yes);

  // Invalid because another primary key already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{2}}, IsPrimaryKey::Yes), std::logic_error);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No), std::logic_error);

  table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::No);

  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::No), std::logic_error);
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::Yes), std::logic_error);
}

}  // namespace opossum
