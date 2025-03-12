#include "base_test.hpp"
#include "storage/constraints/table_order_constraint.hpp"
#include "storage/table.hpp"

namespace hyrise {

class TableOrderConstraintTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = Table::create_dummy_table({{"a", DataType::Int, false},
                                        {"b", DataType::Int, false},
                                        {"c", DataType::Int, false},
                                        {"d", DataType::Int, false},
                                        {"e", DataType::Int, false}});
  }

  std::shared_ptr<Table> _table;
};

TEST_F(TableOrderConstraintTest, ConstraintType) {
  const auto order_constraint = TableOrderConstraint({{ColumnID{0}}, {{ColumnID{1}}}});
  EXPECT_EQ(order_constraint.type(), TableConstraintType::Order);
}

TEST_F(TableOrderConstraintTest, OrderedColumnIDs) {
  // Implementation should not mess up the order of the column IDs.
  const auto order_constraint = TableOrderConstraint({{ColumnID{2}, ColumnID{1}}, {{ColumnID{3}, ColumnID{4}}}});
  EXPECT_EQ(order_constraint.ordering_columns().size(), 2);
  EXPECT_EQ(order_constraint.ordering_columns().front(), ColumnID{2});
  EXPECT_EQ(order_constraint.ordering_columns().back(), ColumnID{1});

  EXPECT_EQ(order_constraint.ordered_columns().size(), 2);
  EXPECT_EQ(order_constraint.ordered_columns().front(), ColumnID{3});
  EXPECT_EQ(order_constraint.ordered_columns().back(), ColumnID{4});
}

TEST_F(TableOrderConstraintTest, AddOrderConstraints) {
  EXPECT_EQ(_table->soft_order_constraints().size(), 0);

  const auto order_constraint_1 = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  _table->add_soft_constraint(order_constraint_1);
  EXPECT_EQ(_table->soft_order_constraints().size(), 1);

  const auto order_constraint_2 = TableOrderConstraint{{ColumnID{0}}, {ColumnID{3}}};
  _table->add_soft_constraint(order_constraint_2);
  EXPECT_EQ(_table->soft_order_constraints().size(), 2);

  const auto order_constraint_3 = TableOrderConstraint{{ColumnID{1}}, {ColumnID{0}}};
  _table->add_soft_constraint(order_constraint_3);
  EXPECT_EQ(_table->soft_order_constraints().size(), 3);

  const auto order_constraint_4 = TableOrderConstraint{{ColumnID{2}}, {ColumnID{1}}};
  _table->add_soft_constraint(order_constraint_4);
  EXPECT_EQ(_table->soft_order_constraints().size(), 4);

  EXPECT_TRUE(_table->soft_order_constraints().contains(order_constraint_1));
  EXPECT_TRUE(_table->soft_order_constraints().contains(order_constraint_2));
  EXPECT_TRUE(_table->soft_order_constraints().contains(order_constraint_3));
  EXPECT_TRUE(_table->soft_order_constraints().contains(order_constraint_4));
}

TEST_F(TableOrderConstraintTest, AddOrderConstraintsInvalid) {
  _table->add_soft_constraint(TableOrderConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}});

  // Invalid because the column id is out of range.
  EXPECT_THROW(_table->add_soft_constraint(TableOrderConstraint{{ColumnID{5}}, {ColumnID{1}}}), std::logic_error);

  // Invalid because the ordered column id is out of range.
  EXPECT_THROW(_table->add_soft_constraint(TableOrderConstraint{{ColumnID{1}}, {ColumnID{5}}}), std::logic_error);

  // Invalid because the same order constraint already exists.
  EXPECT_THROW(
      _table->add_soft_constraint(TableOrderConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}}),
      std::logic_error);

  // Invalid because the order constraint with same ordered columns, but more required columns already exists.
  EXPECT_THROW(_table->add_soft_constraint(TableOrderConstraint{{ColumnID{0}}, {ColumnID{2}, ColumnID{3}}}),
               std::logic_error);

  // Invalid because the order constraint with same ordered columns, but fewer required columns already exists.
  EXPECT_THROW(_table->add_soft_constraint(
                   TableOrderConstraint{{ColumnID{0}, ColumnID{1}, ColumnID{4}}, {ColumnID{2}, ColumnID{3}}}),
               std::logic_error);

  // Invalid because the order constraint with same required columns, but more ordered columns already exists.
  EXPECT_THROW(_table->add_soft_constraint(TableOrderConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}}}),
               std::logic_error);

  // Invalid because the order constraint with same required columns, but fewer ordered columns already exists.
  EXPECT_THROW(_table->add_soft_constraint(
                   TableOrderConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}, ColumnID{4}}}),
               std::logic_error);

  if constexpr (HYRISE_DEBUG) {
    // Ordering and ordered columns must be disjoint.
    EXPECT_THROW(TableOrderConstraint({ColumnID{0}, ColumnID{1}}, {ColumnID{1}, ColumnID{2}}), std::logic_error);
  }
}

TEST_F(TableOrderConstraintTest, Equals) {
  const auto order_constraint_a = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}};
  const auto order_constraint_a_copy = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}};
  const auto order_constraint_a_reordered = TableOrderConstraint{{ColumnID{2}, ColumnID{0}}, {ColumnID{1}}};

  const auto order_constraint_b = TableOrderConstraint{{ColumnID{0}, ColumnID{3}}, {ColumnID{1}}};
  const auto order_constraint_c = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  const auto order_constraint_d = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{3}}};

  EXPECT_TRUE(order_constraint_a == order_constraint_a);
  EXPECT_TRUE(order_constraint_a == order_constraint_a_copy);
  EXPECT_TRUE(order_constraint_a_copy == order_constraint_a);
  EXPECT_FALSE(order_constraint_a == order_constraint_a_reordered);
  EXPECT_FALSE(order_constraint_a_reordered == order_constraint_a);

  EXPECT_FALSE(order_constraint_a == order_constraint_b);
  EXPECT_FALSE(order_constraint_b == order_constraint_a);

  EXPECT_FALSE(order_constraint_c == order_constraint_a);
  EXPECT_FALSE(order_constraint_a == order_constraint_c);

  EXPECT_FALSE(order_constraint_d == order_constraint_a);
  EXPECT_FALSE(order_constraint_a == order_constraint_d);
}

TEST_F(TableOrderConstraintTest, Hash) {
  const auto order_constraint_a = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}};
  const auto order_constraint_a_copy = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{1}}};
  const auto order_constraint_a_reordered = TableOrderConstraint{{ColumnID{2}, ColumnID{0}}, {ColumnID{1}}};

  const auto order_constraint_b = TableOrderConstraint{{ColumnID{0}, ColumnID{3}}, {ColumnID{1}}};
  const auto order_constraint_c = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  const auto order_constraint_d = TableOrderConstraint{{ColumnID{0}, ColumnID{2}}, {ColumnID{3}}};

  EXPECT_TRUE(order_constraint_a.hash() == order_constraint_a.hash());
  EXPECT_TRUE(order_constraint_a.hash() == order_constraint_a_copy.hash());
  EXPECT_TRUE(order_constraint_a_copy.hash() == order_constraint_a.hash());
  EXPECT_FALSE(order_constraint_a.hash() == order_constraint_a_reordered.hash());
  EXPECT_FALSE(order_constraint_a_reordered.hash() == order_constraint_a.hash());

  EXPECT_FALSE(order_constraint_a.hash() == order_constraint_b.hash());
  EXPECT_FALSE(order_constraint_b.hash() == order_constraint_a.hash());

  EXPECT_FALSE(order_constraint_c.hash() == order_constraint_a.hash());
  EXPECT_FALSE(order_constraint_a.hash() == order_constraint_c.hash());

  EXPECT_FALSE(order_constraint_d.hash() == order_constraint_a.hash());
  EXPECT_FALSE(order_constraint_a.hash() == order_constraint_d.hash());
}

}  // namespace hyrise
