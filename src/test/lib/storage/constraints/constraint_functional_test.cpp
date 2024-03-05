#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "storage/constraints/constraint_functional.hpp"
#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/constraints/table_order_constraint.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

using namespace constraint_functional;  // NOLINT(build/namespaces)

class ConstraintFunctionalTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = Table::create_dummy_table({{"a", DataType::Int, false},
                                          {"b", DataType::Int, false},
                                          {"c", DataType::Int, false},
                                          {"d", DataType::Int, false},
                                          {"e", DataType::Int, false},
                                          {"f", DataType::Int, false}});
    _table_b = Table::create_dummy_table({{"x", DataType::Int, false}, {"y", DataType::Int, false}});
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
};

TEST_F(ConstraintFunctionalTest, TableKeyConstraint) {
  primary_key(_table_a, {"a", "b"});

  EXPECT_EQ(_table_a->soft_key_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_key_constraints().contains(
      TableKeyConstraint({ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY)));

  unique(_table_a, {"c"});

  EXPECT_EQ(_table_a->soft_key_constraints().size(), 2);
  EXPECT_TRUE(_table_a->soft_key_constraints().contains(TableKeyConstraint({ColumnID{2}}, KeyConstraintType::UNIQUE)));

  // There is already a primary key set.
  EXPECT_THROW(primary_key(_table_a, {"e"}), std::logic_error);

  // Table ist not set.
  EXPECT_THROW(unique(nullptr, {"a"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(primary_key(_table_b, {}), std::logic_error);
  EXPECT_THROW(unique(_table_b, {}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(primary_key(_table_b, {"foo"}), std::logic_error);
  EXPECT_THROW(unique(_table_b, {"foo"}), std::logic_error);

  // There are already constraints on the requested columns.
  EXPECT_THROW(primary_key(_table_a, {"a"}), std::logic_error);
  EXPECT_THROW(unique(_table_a, {"a"}), std::logic_error);
  EXPECT_THROW(primary_key(_table_a, {"c"}), std::logic_error);
  EXPECT_THROW(unique(_table_a, {"c"}), std::logic_error);
  EXPECT_THROW(primary_key(_table_a, {"b", "d"}), std::logic_error);
  EXPECT_THROW(unique(_table_a, {"b", "d"}), std::logic_error);
  EXPECT_THROW(primary_key(_table_a, {"c", "d"}), std::logic_error);
  EXPECT_THROW(unique(_table_a, {"c", "d"}), std::logic_error);
}

TEST_F(ConstraintFunctionalTest, ForeignKeyConstraint) {
  foreign_key(_table_a, {"a", "b"}, _table_b, {"x", "y"});

  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(
      ForeignKeyConstraint({ColumnID{0}, ColumnID{1}}, _table_a, {ColumnID{0}, ColumnID{1}}, _table_b)));
  EXPECT_EQ(_table_b->referenced_foreign_key_constraints().size(), 1);
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(
      ForeignKeyConstraint({ColumnID{0}, ColumnID{1}}, _table_a, {ColumnID{0}, ColumnID{1}}, _table_b)));

  // Number of columns does not match.
  EXPECT_THROW(foreign_key(_table_a, {"c", "d"}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {"c"}, _table_b, {"x", "y"}), std::logic_error);

  // Table ist not set.
  EXPECT_THROW(foreign_key(nullptr, {"c"}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {"c"}, nullptr, {"y"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(foreign_key(_table_a, {}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {"c"}, _table_b, {}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {}, _table_b, {}), std::logic_error);

  // Foreign key references the same table.
  EXPECT_THROW(foreign_key(_table_a, {"c"}, _table_a, {"d"}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(foreign_key(_table_a, {"c"}, _table_b, {"foo"}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {"foo"}, _table_b, {"y"}), std::logic_error);

  // The constraint already exists.
  EXPECT_THROW(foreign_key(_table_a, {"a", "b"}, _table_b, {"x", "y"}), std::logic_error);
  EXPECT_THROW(foreign_key(_table_a, {"b", "a"}, _table_b, {"y", "x"}), std::logic_error);
}

TEST_F(ConstraintFunctionalTest, TableOrderConstraint) {
  order(_table_a, {"a", "b"}, {"c", "d"});

  EXPECT_EQ(_table_a->soft_order_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_order_constraints().contains(
      TableOrderConstraint({ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}})));

  // Table ist not set.
  EXPECT_THROW(order(nullptr, {"e"}, {"f"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(order(_table_a, {}, {"f"}), std::logic_error);
  EXPECT_THROW(order(_table_a, {"e"}, {}), std::logic_error);
  EXPECT_THROW(order(_table_a, {}, {}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(order(_table_a, {"foo"}, {"e"}), std::logic_error);
  EXPECT_THROW(order(_table_a, {"e"}, {"foo"}), std::logic_error);

  // There are already constraints on the requested columns.
  EXPECT_THROW(order(_table_a, {"e", "b"}, {"c", "d"}), std::logic_error);
  EXPECT_THROW(order(_table_a, {"e", "a"}, {"c", "d"}), std::logic_error);
  EXPECT_THROW(order(_table_a, {"a", "b"}, {"e", "c"}), std::logic_error);
  EXPECT_THROW(order(_table_a, {"a", "b"}, {"c", "e"}), std::logic_error);
}

}  // namespace hyrise
