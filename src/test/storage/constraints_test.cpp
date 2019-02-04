#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
class ConstraintsTest : public BaseTest {
 protected:
  void SetUp() override {
    // construct temporary table with no nullable columns
    column_definitions.emplace_back("column0", DataType::Int);
    column_definitions.emplace_back("column1", DataType::Int);
    column_definitions.emplace_back("column2", DataType::Int);

    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    auto& manager = StorageManager::get();
    manager.add_table("table_temp", table_temp);

    table_temp->append({1, 1, 3});
    table_temp->append({2, 1, 2});
    table_temp->append({3, 2, 0});

    auto gt = std::make_shared<GetTable>("table_temp");
    gt->execute();

    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    table->add_unique_constraint({ColumnID{0}});
    manager.add_table("table", table);
    auto table_insert = std::make_shared<Insert>("table", gt);
    auto table_context = TransactionManager::get().new_transaction_context();
    table_insert->set_transaction_context(table_context);
    table_insert->execute();
    table_context->commit();

    // construct temporary table with only nullable columns
    nullable_column_definitions.emplace_back("column0", DataType::Int, true);
    nullable_column_definitions.emplace_back("column1", DataType::Int, true);
    nullable_column_definitions.emplace_back("column2", DataType::Int, true);

    auto table_temp_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 3, UseMvcc::Yes);
    manager.add_table("table_temp_nullable", table_temp_nullable);

    table_temp_nullable->append({1, 1, 3});
    table_temp_nullable->append({2, 1, 2});
    table_temp_nullable->append({3, 2, 0});

    auto gt_nullable = std::make_shared<GetTable>("table_temp_nullable");
    gt_nullable->execute();

    auto table_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 3, UseMvcc::Yes);
    table_nullable->add_unique_constraint({ColumnID{0}});
    manager.add_table("table_nullable", table_nullable);
    auto table_nullable_insert = std::make_shared<Insert>("table_nullable", gt_nullable);
    auto table_nullable_context = TransactionManager::get().new_transaction_context();
    table_nullable_insert->set_transaction_context(table_nullable_context);
    table_nullable_insert->execute();
    table_nullable_context->commit();
  }

  TableColumnDefinitions column_definitions;
  TableColumnDefinitions nullable_column_definitions;
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto table_nullable = manager.get_table("table_nullable");

  // invalid because column id is out of range
  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);

  // invalid because column must be non nullable for primary constraint
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{1}}, true), std::exception);

  // invalid because there is still a nullable column
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);

  // invalid because the column contains duplicated values.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}), std::exception);

  table->add_unique_constraint({ColumnID{2}}, true);

  // invalid because a primary key already exists.
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

  // invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}}), std::exception);

  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  // invalid because a constraint on the same columns already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}, ColumnID{2}}), std::exception);
}

TEST_F(ConstraintsTest, ValidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertNullable) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});
  new_values->append({NullValue{}, 1, 3});
  new_values->append({NullValue{}, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertNullable) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({NullValue{}, 1, 3});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertConcatenated) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 4});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertConcatenated) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertNullableConcatenated) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 4});
  new_values->append({NullValue{}, 1, 5});
  new_values->append({NullValue{}, 1, 6});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertNullableConcatenated) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 5});
  new_values->append({1, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, InvalidInsertDeleteRace) {
  // This test simulates the situation where one transaction wants to add an already existing
  // value that is deleted by another transaction at the same time. Both operators only commit
  // successfully if the delete operator COMMITS before the insert operator is EXECUTED
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values but do NOT commit
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  insert->set_transaction_context(insert_context);
  insert->execute();
  EXPECT_TRUE(insert->execute_failed());
  EXPECT_TRUE(insert_context->rollback());

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  EXPECT_TRUE(del_transaction_context->commit());
}

TEST_F(ConstraintsTest, ValidInsertDeleteRace) {
  // This test simulates the situation where one transaction wants to add an already existing
  // value that is deleted by another transaction at the same time. Both operators only commit
  // successfully if the delete operator COMMITS before the insert operator is EXECUTED
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value and commit directly
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();

  EXPECT_FALSE(delete_op->execute_failed());
  EXPECT_TRUE(del_transaction_context->commit());

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  insert->set_transaction_context(insert_context);
  insert->execute();

  EXPECT_FALSE(insert->execute_failed());
  EXPECT_TRUE(insert_context->commit());
}

TEST_F(ConstraintsTest, InsertInsertRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);
  new_values->append({5, 0, 1});

  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();

  // Both operators want to insert the same value
  auto insert_1 = std::make_shared<Insert>("table", gt);
  auto insert_2 = std::make_shared<Insert>("table", gt);

  auto insert_1_context = TransactionManager::get().new_transaction_context();
  auto insert_2_context = TransactionManager::get().new_transaction_context();
  insert_1->set_transaction_context(insert_1_context);
  insert_2->set_transaction_context(insert_2_context);

  // They both execute successfully since the value was not commited by either of them at the point of execution
  insert_1->execute();
  EXPECT_FALSE(insert_1->execute_failed());
  insert_2->execute();
  EXPECT_FALSE(insert_2->execute_failed());

  // Only the first commit is successfully, the other transaction sees the inserted value at the point of commiting
  EXPECT_TRUE(insert_1_context->commit());
  EXPECT_FALSE(insert_2_context->commit());
  EXPECT_TRUE(insert_2_context->rollback());
}

}  // namespace opossum
