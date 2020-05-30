#include <memory>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "storage/constraints/table_unique_constraint.hpp"
#include "storage/table.hpp"

namespace opossum {

class TableUniqueConstraintTest : public BaseTest {
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

TEST_F(TableUniqueConstraintTest, InvalidConstraintAdd) {
  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("table");
  auto table_nullable = sm.get_table("table_nullable");

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{5}}, KeyConstraintType::UNIQUE}), std::logic_error);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({{ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  table->add_soft_unique_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});

  // Invalid because another primary key already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}), std::logic_error);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE}), std::logic_error);

  table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE});

  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE}),
               std::logic_error);
  EXPECT_THROW(table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);
}

TEST_F(UniqueConstraintsTest, Equals) {
  const auto unique_constraint_a = TableUniqueConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto primary_key_constraint_a =
      TableUniqueConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY};
  const auto unique_constraint_b = TableUniqueConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};
  const auto unique_constraint_c = TableUniqueConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};

  EXPECT_TRUE(unique_constraint_a == unique_constraint_a);

  EXPECT_FALSE(unique_constraint_a == primary_key_constraint_a);
  EXPECT_FALSE(primary_key_constraint_a == unique_constraint_a);

  EXPECT_FALSE(unique_constraint_a == unique_constraint_b);
  EXPECT_FALSE(unique_constraint_b == unique_constraint_a);

  EXPECT_FALSE(unique_constraint_c == unique_constraint_a);
  EXPECT_FALSE(unique_constraint_a == unique_constraint_c);
}

}  // namespace opossum
