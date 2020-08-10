#include <memory>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "storage/table.hpp"
#include "storage/table_key_constraint.hpp"

namespace opossum {

class TableKeyConstraintTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, false);
      column_definitions.emplace_back("column2", DataType::Int, false);
      column_definitions.emplace_back("column3", DataType::Int, false);
      _table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      sm.add_table("table", _table);
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      _table_nullable = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      sm.add_table("table_nullable", _table_nullable);
    }
  }

  std::shared_ptr<Table> _table;
  std::shared_ptr<Table> _table_nullable;
};

TEST_F(TableKeyConstraintTest, DuplicateColumnIDs) {
  // Implementation should avoid duplicate ColumnIDs
  const auto key_constraint = TableKeyConstraint({{ColumnID{1}, ColumnID{1}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(key_constraint.columns().size(), 1);
  EXPECT_EQ(*key_constraint.columns().begin(), ColumnID{1});
}

TEST_F(TableKeyConstraintTest, AddKeyConstraints) {
  EXPECT_EQ(_table->soft_key_constraints().size(), 0);
  _table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(_table->soft_key_constraints().size(), 1);
  _table->add_soft_key_constraint({{ColumnID{1}, ColumnID{2}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(_table->soft_key_constraints().size(), 2);

  const auto& key_constraint_1 = *_table->soft_key_constraints().cbegin();
  EXPECT_EQ(key_constraint_1.columns().size(), 1);
  EXPECT_EQ(*key_constraint_1.columns().begin(), ColumnID{0});

  const auto& key_constraint_2 = *(++_table->soft_key_constraints().cbegin());
  EXPECT_EQ(key_constraint_2.columns().size(), 2);
  EXPECT_TRUE(key_constraint_2.columns().contains(ColumnID{1}) && key_constraint_2.columns().contains(ColumnID{2}));
}

TEST_F(TableKeyConstraintTest, AddKeyConstraintsInvalid) {
  _table_nullable->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  _table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});

  // Invalid, because the column id is out of range
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{5}}, KeyConstraintType::UNIQUE}), std::logic_error);

  // Invalid, because the column must be non nullable for a PRIMARY KEY.
  EXPECT_THROW(_table_nullable->add_soft_key_constraint({{ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  // Invalid, because there is still a nullable column.
  EXPECT_THROW(_table_nullable->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);

  _table->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});
  _table->add_soft_key_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE});

  // Invalid, because PRIMARY KEY already exists.
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}), std::logic_error);

  // Invalid, because key constraints for the given column sets already exist
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE}), std::logic_error);
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY}), std::logic_error);
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE}),
               std::logic_error);
  EXPECT_THROW(_table->add_soft_key_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY}),
               std::logic_error);
}

TEST_F(TableKeyConstraintTest, Equals) {
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_reordered = TableKeyConstraint{{ColumnID{2}, ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto primary_key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY};

  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};

  EXPECT_TRUE(key_constraint_a == key_constraint_a);
  EXPECT_TRUE(key_constraint_a == key_constraint_a_reordered);
  EXPECT_TRUE(key_constraint_a_reordered == key_constraint_a);

  EXPECT_FALSE(key_constraint_a == primary_key_constraint_a);
  EXPECT_FALSE(primary_key_constraint_a == key_constraint_a);

  EXPECT_FALSE(key_constraint_a == key_constraint_b);
  EXPECT_FALSE(key_constraint_b == key_constraint_a);

  EXPECT_FALSE(key_constraint_c == key_constraint_a);
  EXPECT_FALSE(key_constraint_a == key_constraint_c);
}

}  // namespace opossum
