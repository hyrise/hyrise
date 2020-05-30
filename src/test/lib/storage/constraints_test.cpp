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

      table->add_soft_unique_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table_nullable", table);

      table->add_soft_unique_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
    }
  }
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("table");
  auto table_nullable = sm.get_table("table_nullable");

  // TODO(Julian) Update test: Invalid because the constraint contains duplicated columns.
  //  auto count_before = table->get_soft_unique_constraints().size();
  //  table->add_soft_unique_constraint({{ColumnID{1}, ColumnID{1}}, {ConstraintType::UNIQUE}});
  //  EXPECT_EQ(table->get_soft_unique_constraints().size(), count_before + 1);

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{5}}, KeyConstraintType::UNIQUE}),
               std::logic_error);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({{ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint(
                   TableConstraintDefinition{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  table->add_soft_unique_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});

  // Invalid because another primary key already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE}),
               std::logic_error);

  table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE});

  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(
      table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE}),
      std::logic_error);
  EXPECT_THROW(
      table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}),
      std::logic_error);
}

TEST_F(ConstraintsTest, Equals) {
  const auto constraint_a = TableUniqueConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto constraint_a_pk = TableUniqueConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY};
  const auto constraint_b = TableUniqueConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};
  const auto constraint_c = TableUniqueConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};

  EXPECT_TRUE(constraint_a == constraint_a);

  EXPECT_FALSE(constraint_a == constraint_a_pk);
  EXPECT_FALSE(constraint_a_pk == constraint_a);

  EXPECT_FALSE(constraint_a == constraint_b);
  EXPECT_FALSE(constraint_b == constraint_a);

  EXPECT_FALSE(constraint_c == constraint_a);
  EXPECT_FALSE(constraint_a == constraint_c);
}

}  // namespace opossum
