#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

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

      table->add_unique_constraint({ColumnID{0}}); 
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table_nullable", table);

      table->add_unique_constraint({ColumnID{0}});
    }
  }
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("table");
  auto table_nullable = sm.get_table("table_nullable");

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);

  // Invalid because the constraint contains duplicated columns.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}, ColumnID{1}}), std::exception);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{1}}, true), std::exception);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);

  table->add_unique_constraint({ColumnID{2}}, true);

  // Invalid because another primary key already exists.
  EXPECT_THROW(
      {
        try {
          table->add_unique_constraint({ColumnID{2}}, true);
        } catch (const std::exception& e) {
          // and this tests that it has the correct message
          EXPECT_TRUE(std::string(e.what()).find("Another primary key already exists for this table.") !=
                      std::string::npos);
          throw;
        }
      },
      std::exception);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}}), std::exception);

  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}, ColumnID{2}}), std::exception);
}

}  // namespace opossum
