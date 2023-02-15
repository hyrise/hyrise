#include "base_test.hpp"

#include <numeric>

#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/table.hpp"

namespace hyrise {

class ForeignKeyConstraintTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = Table::create_dummy_table({{"a", DataType::Int, false},
                                          {"b", DataType::Int, false},
                                          {"c", DataType::Int, false},
                                          {"d", DataType::Int, false}});
    _table_b = Table::create_dummy_table({{"x", DataType::Int, false}, {"y", DataType::Int, false}});
    _table_c = Table::create_dummy_table({{"u", DataType::Int, false}, {"v", DataType::Int, false}});
  }

  std::shared_ptr<Table> _table_a, _table_b, _table_c;
};

TEST_F(ForeignKeyConstraintTest, OrderedColumnIDs) {
  // To handle equivalent foreign key constraints / INDs with swapped columns, we sort the columns and apply the
  // permutation to the foreign key columns.
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{ColumnID{2}, ColumnID{1}}, {ColumnID{3}, ColumnID{4}}, _table_a, _table_b};
  EXPECT_EQ(foreign_key_constraint.columns().size(), 2);
  EXPECT_EQ(foreign_key_constraint.columns().front(), ColumnID{1});
  EXPECT_EQ(foreign_key_constraint.columns().back(), ColumnID{2});

  EXPECT_EQ(foreign_key_constraint.foreign_key_columns().size(), 2);
  EXPECT_EQ(foreign_key_constraint.foreign_key_columns().front(), ColumnID{4});
  EXPECT_EQ(foreign_key_constraint.foreign_key_columns().back(), ColumnID{3});

  // Try a larger one.
  auto column_ids = std::vector<ColumnID>{100};
  std::iota(column_ids.begin(), column_ids.end(), ColumnID{0});
  auto reverse_column_ids = column_ids;
  std::sort(reverse_column_ids.begin(), reverse_column_ids.end(), std::greater<ColumnID>{});
  const auto large_foreign_key_constraint =
      ForeignKeyConstraint{reverse_column_ids, reverse_column_ids, _table_a, _table_b};
  EXPECT_EQ(large_foreign_key_constraint.columns(), column_ids);
  EXPECT_EQ(large_foreign_key_constraint.foreign_key_columns(), column_ids);
}

TEST_F(ForeignKeyConstraintTest, AddForeignKeyConstraints) {
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 0);

  const auto foreign_key_constraint_1 = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a};
  _table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a});
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 1);

  const auto foreign_key_constraint_2 = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{3}}, _table_b, _table_a};
  _table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{3}}, _table_b, _table_a});
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 2);

  const auto foreign_key_constraint_3 = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_c, _table_a};
  _table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, _table_c, _table_a});
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 3);

  const auto foreign_key_constraint_4 = ForeignKeyConstraint{{ColumnID{1}}, {ColumnID{0}}, _table_b, _table_a};
  _table_a->add_soft_foreign_key_constraint({{ColumnID{1}}, {ColumnID{0}}, _table_b, _table_a});
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 4);

  const auto foreign_key_constraint_5 =
      ForeignKeyConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{2}, ColumnID{3}}, _table_b, _table_a};
  _table_a->add_soft_foreign_key_constraint(
      {{ColumnID{1}, ColumnID{0}}, {ColumnID{2}, ColumnID{3}}, _table_b, _table_a});
  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 5);
  // The constraint is the swapped version of the previous constraint, which is equivalent. Thus, it cannot be added as
  // a duplicate.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint(
                   {{ColumnID{0}, ColumnID{1}}, {ColumnID{3}, ColumnID{2}}, _table_b, _table_a}),
               std::logic_error);

  // Ensure all constraints were added.
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(foreign_key_constraint_1));
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(foreign_key_constraint_2));
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(foreign_key_constraint_3));
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(foreign_key_constraint_4));
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(foreign_key_constraint_5));

  // Ensure all constraints were also added to the other table.
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(foreign_key_constraint_1));
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(foreign_key_constraint_2));
  EXPECT_TRUE(_table_c->referenced_foreign_key_constraints().contains(foreign_key_constraint_3));
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(foreign_key_constraint_4));
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(foreign_key_constraint_5));
}

TEST_F(ForeignKeyConstraintTest, AddInclusionConstraintsInvalid) {
  _table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a});

  // Invalid because the column id is out of range.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint({{ColumnID{5}}, {ColumnID{1}}, _table_b, _table_a}),
               std::logic_error);

  // Invalid because the column id of the referenced table is out of range.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint({{ColumnID{1}}, {ColumnID{5}}, _table_b, _table_a}),
               std::logic_error);

  // Invalid because inclusion constraint for the given column sets and referenced table already exists.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a}),
               std::logic_error);

  // Invalid,because the referenced table does not exist.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, nullptr, _table_a}),
               std::logic_error);

  // Invalid because the referenced table is not the table we add the constraint to.
  EXPECT_THROW(_table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_c}),
               std::logic_error);

  // Invalid because the column lists have different sizes.
  EXPECT_THROW(
      _table_a->add_soft_foreign_key_constraint({{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}, _table_b, _table_a}),
      std::logic_error);
}

TEST_F(ForeignKeyConstraintTest, Equals) {
  const auto foreign_key_constraint_a = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_a, _table_b};
  const auto foreign_key_constraint_a_copy = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_a, _table_b};

  const auto foreign_key_constraint_b = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{2}}, _table_a, _table_b};
  const auto foreign_key_constraint_c = ForeignKeyConstraint{{ColumnID{1}}, {ColumnID{0}}, _table_a, _table_b};
  const auto foreign_key_constraint_d = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a};
  const auto foreign_key_constraint_e =
      ForeignKeyConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}, _table_a, _table_b};
  const auto foreign_key_constraint_e_reordered =
      ForeignKeyConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{3}, ColumnID{2}}, _table_a, _table_b};
  const auto foreign_key_constraint_f =
      ForeignKeyConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{2}, ColumnID{3}}, _table_a, _table_b};

  EXPECT_TRUE(foreign_key_constraint_a == foreign_key_constraint_a);
  EXPECT_TRUE(foreign_key_constraint_a == foreign_key_constraint_a_copy);
  EXPECT_TRUE(foreign_key_constraint_a_copy == foreign_key_constraint_a);

  EXPECT_FALSE(foreign_key_constraint_a == foreign_key_constraint_b);
  EXPECT_FALSE(foreign_key_constraint_b == foreign_key_constraint_a);

  EXPECT_FALSE(foreign_key_constraint_c == foreign_key_constraint_a);
  EXPECT_FALSE(foreign_key_constraint_a == foreign_key_constraint_c);

  EXPECT_FALSE(foreign_key_constraint_d == foreign_key_constraint_a);
  EXPECT_FALSE(foreign_key_constraint_a == foreign_key_constraint_d);

  EXPECT_FALSE(foreign_key_constraint_e == foreign_key_constraint_a);
  EXPECT_FALSE(foreign_key_constraint_a == foreign_key_constraint_e);
  EXPECT_TRUE(foreign_key_constraint_e == foreign_key_constraint_e_reordered);
  EXPECT_TRUE(foreign_key_constraint_e_reordered == foreign_key_constraint_e);

  EXPECT_FALSE(foreign_key_constraint_e == foreign_key_constraint_f);
  EXPECT_FALSE(foreign_key_constraint_f == foreign_key_constraint_e);
}

TEST_F(ForeignKeyConstraintTest, Hash) {
  const auto foreign_key_constraint_a = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_a, _table_b};
  const auto foreign_key_constraint_a_copy = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_a, _table_b};

  const auto foreign_key_constraint_b = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{2}}, _table_a, _table_b};
  const auto foreign_key_constraint_c = ForeignKeyConstraint{{ColumnID{1}}, {ColumnID{0}}, _table_a, _table_b};
  const auto foreign_key_constraint_d = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{1}}, _table_b, _table_a};
  const auto foreign_key_constraint_e =
      ForeignKeyConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}, _table_a, _table_b};
  const auto foreign_key_constraint_e_reordered =
      ForeignKeyConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{3}, ColumnID{2}}, _table_a, _table_b};
  const auto foreign_key_constraint_f =
      ForeignKeyConstraint{{ColumnID{1}, ColumnID{0}}, {ColumnID{2}, ColumnID{3}}, _table_a, _table_b};

  EXPECT_TRUE(foreign_key_constraint_a.hash() == foreign_key_constraint_a.hash());
  EXPECT_TRUE(foreign_key_constraint_a.hash() == foreign_key_constraint_a_copy.hash());
  EXPECT_TRUE(foreign_key_constraint_a_copy.hash() == foreign_key_constraint_a.hash());

  EXPECT_FALSE(foreign_key_constraint_a.hash() == foreign_key_constraint_b.hash());
  EXPECT_FALSE(foreign_key_constraint_b.hash() == foreign_key_constraint_a.hash());

  EXPECT_FALSE(foreign_key_constraint_c.hash() == foreign_key_constraint_a.hash());
  EXPECT_FALSE(foreign_key_constraint_a.hash() == foreign_key_constraint_c.hash());

  EXPECT_FALSE(foreign_key_constraint_d.hash() == foreign_key_constraint_a.hash());
  EXPECT_FALSE(foreign_key_constraint_a.hash() == foreign_key_constraint_d.hash());

  EXPECT_FALSE(foreign_key_constraint_e.hash() == foreign_key_constraint_a.hash());
  EXPECT_FALSE(foreign_key_constraint_a.hash() == foreign_key_constraint_e.hash());
  EXPECT_TRUE(foreign_key_constraint_e.hash() == foreign_key_constraint_e_reordered.hash());
  EXPECT_TRUE(foreign_key_constraint_e_reordered.hash() == foreign_key_constraint_e.hash());

  EXPECT_FALSE(foreign_key_constraint_e.hash() == foreign_key_constraint_f.hash());
  EXPECT_FALSE(foreign_key_constraint_f.hash() == foreign_key_constraint_e.hash());
}

}  // namespace hyrise
