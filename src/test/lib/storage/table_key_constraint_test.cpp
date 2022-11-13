#include <memory>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "storage/table.hpp"
#include "storage/table_key_constraint.hpp"

namespace hyrise {

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
      _table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

      sm.add_table("table", _table);
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      _table_nullable = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

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

  const auto key_constraint_1 = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  _table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(_table->soft_key_constraints().size(), 1);

  const auto key_constraint_2 = TableKeyConstraint{{ColumnID{1}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  _table->add_soft_key_constraint({{ColumnID{1}, ColumnID{2}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(_table->soft_key_constraints().size(), 2);

  EXPECT_TRUE(_table->soft_key_constraints().contains(key_constraint_1));
  EXPECT_TRUE(_table->soft_key_constraints().contains(key_constraint_2));
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

TEST_F(TableKeyConstraintTest, Hash) {
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_reordered = TableKeyConstraint{{ColumnID{2}, ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto primary_key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY};

  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};

  EXPECT_TRUE(key_constraint_a.hash() == key_constraint_a.hash());
  EXPECT_TRUE(key_constraint_a.hash() == key_constraint_a_reordered.hash());
  EXPECT_TRUE(key_constraint_a_reordered.hash() == key_constraint_a.hash());

  EXPECT_FALSE(key_constraint_a.hash() == primary_key_constraint_a.hash());
  EXPECT_FALSE(primary_key_constraint_a.hash() == key_constraint_a.hash());

  EXPECT_FALSE(key_constraint_a.hash() == key_constraint_b.hash());
  EXPECT_FALSE(key_constraint_b.hash() == key_constraint_a.hash());

  EXPECT_FALSE(key_constraint_c.hash() == key_constraint_a.hash());
  EXPECT_FALSE(key_constraint_a.hash() == key_constraint_c.hash());
}

TEST_F(TableKeyConstraintTest, Less) {
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_reordered = TableKeyConstraint{{ColumnID{2}, ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto primary_key_constraint_a = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY};

  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};

  EXPECT_FALSE(key_constraint_a < key_constraint_a);
  EXPECT_FALSE(key_constraint_a < key_constraint_a_reordered);

  EXPECT_TRUE(primary_key_constraint_a < key_constraint_a);

  EXPECT_TRUE(key_constraint_a < key_constraint_b);

  EXPECT_TRUE(key_constraint_c < key_constraint_a);
}

TEST_F(TableKeyConstraintTest, OrderIndependence) {
  const auto key_constraint_1 = TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_2 = TableKeyConstraint{{ColumnID{2}, ColumnID{3}}, KeyConstraintType::UNIQUE};

  auto key_constraints_a = TableKeyConstraints{};
  auto key_constraints_b = TableKeyConstraints{};

  key_constraints_a.insert(key_constraint_1);
  key_constraints_a.insert(key_constraint_2);

  key_constraints_b.insert(key_constraint_2);
  key_constraints_b.insert(key_constraint_1);

  EXPECT_EQ(key_constraints_a, key_constraints_b);
  EXPECT_EQ(*key_constraints_a.begin(), *key_constraints_b.begin());
  EXPECT_EQ(*std::next(key_constraints_a.begin()), *std::next(key_constraints_b.begin()));
}

}  // namespace hyrise
