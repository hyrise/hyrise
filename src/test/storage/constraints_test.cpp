#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

std::tuple<std::shared_ptr<Insert>, std::shared_ptr<TransactionContext>> _insert_values(
    std::string table_name, std::shared_ptr<Table> new_values) {
  auto& manager = StorageManager::get();
  manager.add_table("new_values", new_values);

  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  manager.drop_table("new_values");

  return std::make_tuple(ins, context);
}

class ConstraintsTest : public BaseTest {
 protected:
  // void SetUp() override {
  //   // First a test table with nonnullable columns is created. This table can be reused in all test as a base table.
  //   column_definitions.emplace_back("column0", DataType::Int);
  //   column_definitions.emplace_back("column1", DataType::Int);
  //   column_definitions.emplace_back("column2", DataType::Int);
  //   column_definitions.emplace_back("column3", DataType::Int);
  //   auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

  //   auto& manager = StorageManager::get();
  //   manager.add_table("table", table);

  //   // The values are added with an insert operator to generate MVCC data.
  //   auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
  //   new_values->append({1, 1, 3, 1});
  //   new_values->append({2, 1, 2, 1});
  //   new_values->append({3, 2, 0, 2});

  //   // NOLINTNEXTLINE(whitespace/braces)
  //   auto [_1, context1] = _insert_values("table", new_values);
  //   context1->commit();

  //   // Initially a unique constraint is defined on a single column since this can be used in all tests
  //   table->add_unique_constraint({ColumnID{0}});

  //   // Next, a test table with nullable columns is created. This table can be reused in all test as a base table
  //   nullable_column_definitions.emplace_back("column0", DataType::Int, true);
  //   nullable_column_definitions.emplace_back("column1", DataType::Int, true);
  //   nullable_column_definitions.emplace_back("column2", DataType::Int, true);
  //   nullable_column_definitions.emplace_back("column3", DataType::Int, true);

  //   auto table_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  //   manager.add_table("table_nullable", table_nullable);

  //   // NOLINTNEXTLINE(whitespace/braces)
  //   auto [_2, context2] = _insert_values("table_nullable", new_values);
  //   context2->commit();

  //   // Initially one for one column a unique constraint is defined since this can be used in all tests
  //   table_nullable->add_unique_constraint({ColumnID{0}});
  // }
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto table_nullable = manager.get_table("table_nullable");

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);

  // Invalid because the constraint contains duplicated columns.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}, ColumnID{1}}), std::exception);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{1}}, true), std::exception);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);

  // Invalid because the column contains duplicated values.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}), std::exception);

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
