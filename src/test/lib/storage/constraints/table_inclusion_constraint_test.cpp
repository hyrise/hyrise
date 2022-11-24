#include "base_test.hpp"

#include "storage/constraints/table_inclusion_constraint.hpp"
#include "storage/table.hpp"

namespace hyrise {

class TableInclusionConstraintTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    {
      auto column_definitions = TableColumnDefinitions{};
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, false);
      column_definitions.emplace_back("column2", DataType::Int, false);
      column_definitions.emplace_back("column3", DataType::Int, false);
      _table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

      sm.add_table("table", _table);
    }

    {
      auto column_definitions = TableColumnDefinitions{};
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      _table_nullable = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

      sm.add_table("table_nullable", _table_nullable);
    }
  }

  std::shared_ptr<Table> _table;
  std::shared_ptr<Table> _table_nullable;
};

TEST_F(TableInclusionConstraintTest, OrderedColumnIDs) {
  // Implementation should not mess up the order of the column IDs.
  const auto inclusion_constraint =
      TableInclusionConstraint({{ColumnID{2}, ColumnID{1}}, {{ColumnID{3}, ColumnID{4}}}, "table_a"});
  EXPECT_EQ(inclusion_constraint.columns().size(), 2);
  EXPECT_EQ(inclusion_constraint.columns().front(), ColumnID{2});
  EXPECT_EQ(inclusion_constraint.columns().back(), ColumnID{1});

  EXPECT_EQ(inclusion_constraint.included_columns().size(), 2);
  EXPECT_EQ(inclusion_constraint.included_columns().front(), ColumnID{3});
  EXPECT_EQ(inclusion_constraint.included_columns().back(), ColumnID{4});
}

TEST_F(TableInclusionConstraintTest, AddInclusionConstraints) {
  EXPECT_EQ(_table->soft_inclusion_constraints().size(), 0);

  const auto inclusion_constraint_1 = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table"};
  _table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{1}}, "table"});
  EXPECT_EQ(_table->soft_inclusion_constraints().size(), 1);

  const auto inclusion_constraint_2 = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{3}}, "table"};
  _table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{3}}, "table"});
  EXPECT_EQ(_table->soft_inclusion_constraints().size(), 2);

  const auto inclusion_constraint_3 = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_nullable"};
  _table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{1}}, "table_nullable"});
  EXPECT_EQ(_table->soft_inclusion_constraints().size(), 3);

  const auto inclusion_constraint_4 = TableInclusionConstraint{{ColumnID{1}}, {ColumnID{0}}, "table"};
  _table->add_soft_inclusion_constraint({{ColumnID{1}}, {ColumnID{0}}, "table"});
  EXPECT_EQ(_table->soft_inclusion_constraints().size(), 4);

  EXPECT_TRUE(_table->soft_inclusion_constraints().contains(inclusion_constraint_1));
  EXPECT_TRUE(_table->soft_inclusion_constraints().contains(inclusion_constraint_2));
  EXPECT_TRUE(_table->soft_inclusion_constraints().contains(inclusion_constraint_3));
  EXPECT_TRUE(_table->soft_inclusion_constraints().contains(inclusion_constraint_4));
}

TEST_F(TableInclusionConstraintTest, AddInclusionConstraintsInvalid) {
  _table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{1}}, "table_nullable"});

  // Invalid, because the column id is out of range
  EXPECT_THROW(_table->add_soft_inclusion_constraint({{ColumnID{5}}, {ColumnID{1}}, "table_nullable"}),
               std::logic_error);

  // Invalid, because the column id of the referenced table is out of range
  EXPECT_THROW(_table->add_soft_inclusion_constraint({{ColumnID{1}}, {ColumnID{5}}, "table_nullable"}),
               std::logic_error);

  // Invalid, because inclusion constraint for the given column sets and referenced table already exists
  EXPECT_THROW(_table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{1}}, "table_nullable"}),
               std::logic_error);

  // Invalid, because the referenced table does not exist
  EXPECT_THROW(_table->add_soft_inclusion_constraint({{ColumnID{0}}, {ColumnID{1}}, "unknown_table"}),
               std::logic_error);

  // Invalid, because the column lists have different sizes
  EXPECT_THROW(_table->add_soft_inclusion_constraint({{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}, "table_nullable"}),
               std::logic_error);
}

TEST_F(TableInclusionConstraintTest, Equals) {
  const auto inclusion_constraint_a = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_a"};
  const auto inclusion_constraint_a_copy = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_a"};

  const auto inclusion_constraint_b = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{2}}, "table_a"};
  const auto inclusion_constraint_c = TableInclusionConstraint{{ColumnID{1}}, {ColumnID{0}}, "table_a"};
  const auto inclusion_constraint_d = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_b"};
  const auto inclusion_constraint_e =
      TableInclusionConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}, "table_a"};
  const auto inclusion_constraint_e_reordered =
      TableInclusionConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{3}, ColumnID{2}}, "table_a"};

  EXPECT_TRUE(inclusion_constraint_a == inclusion_constraint_a);
  EXPECT_TRUE(inclusion_constraint_a == inclusion_constraint_a_copy);
  EXPECT_TRUE(inclusion_constraint_a_copy == inclusion_constraint_a);

  EXPECT_FALSE(inclusion_constraint_a == inclusion_constraint_b);
  EXPECT_FALSE(inclusion_constraint_b == inclusion_constraint_a);

  EXPECT_FALSE(inclusion_constraint_c == inclusion_constraint_a);
  EXPECT_FALSE(inclusion_constraint_a == inclusion_constraint_c);

  EXPECT_FALSE(inclusion_constraint_d == inclusion_constraint_a);
  EXPECT_FALSE(inclusion_constraint_a == inclusion_constraint_d);

  EXPECT_FALSE(inclusion_constraint_e == inclusion_constraint_a);
  EXPECT_FALSE(inclusion_constraint_a == inclusion_constraint_e);
  EXPECT_FALSE(inclusion_constraint_e == inclusion_constraint_e_reordered);
  EXPECT_FALSE(inclusion_constraint_e_reordered == inclusion_constraint_e);
}

TEST_F(TableInclusionConstraintTest, Hash) {
  const auto inclusion_constraint_a = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_a"};
  const auto inclusion_constraint_a_copy = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_a"};

  const auto inclusion_constraint_b = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{2}}, "table_a"};
  const auto inclusion_constraint_c = TableInclusionConstraint{{ColumnID{1}}, {ColumnID{0}}, "table_a"};
  const auto inclusion_constraint_d = TableInclusionConstraint{{ColumnID{0}}, {ColumnID{1}}, "table_b"};
  const auto inclusion_constraint_e =
      TableInclusionConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}, "table_a"};
  const auto inclusion_constraint_e_reordered =
      TableInclusionConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{3}, ColumnID{2}}, "table_a"};

  EXPECT_TRUE(inclusion_constraint_a.hash() == inclusion_constraint_a.hash());
  EXPECT_TRUE(inclusion_constraint_a.hash() == inclusion_constraint_a_copy.hash());
  EXPECT_TRUE(inclusion_constraint_a_copy.hash() == inclusion_constraint_a.hash());

  EXPECT_FALSE(inclusion_constraint_a.hash() == inclusion_constraint_b.hash());
  EXPECT_FALSE(inclusion_constraint_b.hash() == inclusion_constraint_a.hash());

  EXPECT_FALSE(inclusion_constraint_c.hash() == inclusion_constraint_a.hash());
  EXPECT_FALSE(inclusion_constraint_a.hash() == inclusion_constraint_c.hash());

  EXPECT_FALSE(inclusion_constraint_d.hash() == inclusion_constraint_a.hash());
  EXPECT_FALSE(inclusion_constraint_a.hash() == inclusion_constraint_d.hash());

  EXPECT_FALSE(inclusion_constraint_e.hash() == inclusion_constraint_a.hash());
  EXPECT_FALSE(inclusion_constraint_a.hash() == inclusion_constraint_e.hash());
  EXPECT_FALSE(inclusion_constraint_e.hash() == inclusion_constraint_e_reordered.hash());
  EXPECT_FALSE(inclusion_constraint_e_reordered.hash() == inclusion_constraint_e.hash());
}

}  // namespace hyrise
