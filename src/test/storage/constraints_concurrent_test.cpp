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
class ConstraintsConcurrentTest : public BaseTest {
 protected:
  void SetUp() override {
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, true);
    column_definitions.emplace_back("column2", DataType::Int, false);

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

    nullable_column_definitions.emplace_back("column0", DataType::Int, true);
    nullable_column_definitions.emplace_back("column1", DataType::Int, false);
    nullable_column_definitions.emplace_back("column2", DataType::Int, false);

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

TEST_F(ConstraintsConcurrentTest, InvalidConstraintAdd) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");

  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);
  // invalid because column must be non nullable for primary constrain
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}, true), std::exception);
  // invalid because there is still a nullable column
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);
  // invalid because the column contains duplicated values.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}), std::exception);
}

TEST_F(ConstraintsConcurrentTest, ValidInsert) {
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

TEST_F(ConstraintsConcurrentTest, InvalidInsert) {
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

TEST_F(ConstraintsConcurrentTest, ValidInsertNullable) {
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

TEST_F(ConstraintsConcurrentTest, InvalidInsertNullable) {
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

TEST_F(ConstraintsConcurrentTest, ValidInsertConcatenated) {
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

TEST_F(ConstraintsConcurrentTest, InvalidInsertConcatenated) {
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

TEST_F(ConstraintsConcurrentTest, ValidInsertNullableConcatenated) {
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

TEST_F(ConstraintsConcurrentTest, InvalidInsertNullableConcatenated) {
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

TEST_F(ConstraintsConcurrentTest, InvalidInsertDeleteRace) {
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

TEST_F(ConstraintsConcurrentTest, ValidInsertDeleteRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

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

  // add new values but do NOT commit
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  insert->set_transaction_context(insert_context);
  insert->execute();
  EXPECT_FALSE(insert->execute_failed());
  EXPECT_TRUE(insert_context->commit());
}

TEST_F(ConstraintsConcurrentTest, InsertInsertRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);
  new_values->append({5, 0, 1});

  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert_1 = std::make_shared<Insert>("table", gt);
  auto insert_2 = std::make_shared<Insert>("table", gt);

  auto insert_1_context = TransactionManager::get().new_transaction_context();
  auto insert_2_context = TransactionManager::get().new_transaction_context();
  insert_1->set_transaction_context(insert_1_context);
  insert_2->set_transaction_context(insert_2_context);
  insert_1->execute();
  EXPECT_FALSE(insert_1->execute_failed());
  insert_2->execute();
  EXPECT_FALSE(insert_2->execute_failed());

  EXPECT_TRUE(insert_1_context->commit());
  EXPECT_FALSE(insert_2_context->commit());
  EXPECT_TRUE(insert_2_context->rollback());
}

}  // namespace opossum
