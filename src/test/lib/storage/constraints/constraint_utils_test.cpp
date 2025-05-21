#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/constraints/table_order_constraint.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

class ConstraintUtilsTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = Table::create_dummy_table({{"a", DataType::Int, false},
                                          {"b", DataType::Int, false},
                                          {"c", DataType::Int, false},
                                          {"d", DataType::Int, false},
                                          {"e", DataType::Int, false},
                                          {"f", DataType::Int, false}});
    _table_b = std::make_shared<Table>(TableColumnDefinitions{{"x", DataType::Int, false}, {"y", DataType::Int, false}},
                                       TableType::Data, std::nullopt, UseMvcc::Yes);
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
};

TEST_F(ConstraintUtilsTest, TableKeyConstraint) {
  primary_key_constraint(_table_a, {"a", "b"});

  EXPECT_EQ(_table_a->soft_key_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_key_constraints().contains(
      TableKeyConstraint({ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY)));

  unique_constraint(_table_a, {"c"});

  EXPECT_EQ(_table_a->soft_key_constraints().size(), 2);
  EXPECT_TRUE(_table_a->soft_key_constraints().contains(TableKeyConstraint({ColumnID{2}}, KeyConstraintType::UNIQUE)));

  // There is already a primary key set.
  EXPECT_THROW(primary_key_constraint(_table_a, {"e"}), std::logic_error);

  // Table is not set.
  EXPECT_THROW(unique_constraint(nullptr, {"a"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(primary_key_constraint(_table_b, {}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_b, {}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(primary_key_constraint(_table_b, {"foo"}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_b, {"foo"}), std::logic_error);

  // There are already constraints on the requested columns.
  EXPECT_THROW(primary_key_constraint(_table_a, {"a"}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_a, {"a"}), std::logic_error);
  EXPECT_THROW(primary_key_constraint(_table_a, {"c"}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_a, {"c"}),
               std::logic_error);  // Trying to add an already existing unique constraint is tolerated.
  EXPECT_THROW(primary_key_constraint(_table_a, {"b", "d"}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_a, {"b", "d"}), std::logic_error);
  EXPECT_THROW(primary_key_constraint(_table_a, {"c", "d"}), std::logic_error);
  EXPECT_THROW(unique_constraint(_table_a, {"c", "d"}), std::logic_error);
}

TEST_F(ConstraintUtilsTest, ForeignKeyConstraint) {
  foreign_key_constraint(_table_a, {"a", "b"}, _table_b, {"x", "y"});

  EXPECT_EQ(_table_a->soft_foreign_key_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_foreign_key_constraints().contains(
      ForeignKeyConstraint({ColumnID{0}, ColumnID{1}}, _table_a, {ColumnID{0}, ColumnID{1}}, _table_b)));
  EXPECT_EQ(_table_b->referenced_foreign_key_constraints().size(), 1);
  EXPECT_TRUE(_table_b->referenced_foreign_key_constraints().contains(
      ForeignKeyConstraint({ColumnID{0}, ColumnID{1}}, _table_a, {ColumnID{0}, ColumnID{1}}, _table_b)));

  // Number of columns does not match.
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c", "d"}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c"}, _table_b, {"x", "y"}), std::logic_error);

  // Table is not set.
  EXPECT_THROW(foreign_key_constraint(nullptr, {"c"}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c"}, nullptr, {"y"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(foreign_key_constraint(_table_a, {}, _table_b, {"y"}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c"}, _table_b, {}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {}, _table_b, {}), std::logic_error);

  // Foreign key references the same table.
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c"}, _table_a, {"d"}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(foreign_key_constraint(_table_a, {"c"}, _table_b, {"foo"}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {"foo"}, _table_b, {"y"}), std::logic_error);

  // The constraint already exists.
  EXPECT_THROW(foreign_key_constraint(_table_a, {"a", "b"}, _table_b, {"x", "y"}), std::logic_error);
  EXPECT_THROW(foreign_key_constraint(_table_a, {"b", "a"}, _table_b, {"y", "x"}), std::logic_error);
}

TEST_F(ConstraintUtilsTest, TableOrderConstraint) {
  order_constraint(_table_a, {"a", "b"}, {"c", "d"});

  EXPECT_EQ(_table_a->soft_order_constraints().size(), 1);
  EXPECT_TRUE(_table_a->soft_order_constraints().contains(
      TableOrderConstraint({ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}})));

  // Table is not set.
  EXPECT_THROW(order_constraint(nullptr, {"e"}, {"f"}), std::logic_error);

  // Columns are empty.
  EXPECT_THROW(order_constraint(_table_a, {}, {"f"}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {"e"}, {}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {}, {}), std::logic_error);

  // Table has no column with requested name.
  EXPECT_THROW(order_constraint(_table_a, {"foo"}, {"e"}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {"e"}, {"foo"}), std::logic_error);

  // There are already constraints on the requested columns.
  EXPECT_THROW(order_constraint(_table_a, {"e", "b"}, {"c", "d"}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {"e", "a"}, {"c", "d"}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {"a", "b"}, {"e", "c"}), std::logic_error);
  EXPECT_THROW(order_constraint(_table_a, {"a", "b"}, {"c", "e"}), std::logic_error);
}

TEST_F(ConstraintUtilsTest, CheckIfTableKeyConstraintIsKnownToBeValid) {
  EXPECT_TRUE(key_constraint_is_confidently_valid(_table_b, {{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY}));
  EXPECT_TRUE(key_constraint_is_confidently_valid(_table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, MAX_COMMIT_ID}));

  EXPECT_FALSE(key_constraint_is_confidently_valid(
      _table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, CommitID{0}, CommitID{0}}));

  // Manually modify the `max_begin_cid` of the chunk to simulate an insert to the table.
  _table_b->append({0, 1});
  _table_b->get_chunk(ChunkID{0})->mvcc_data()->max_begin_cid = CommitID{2};

  // The first constraint is permanent and therefore valid, the second one was verified on a previous CommitID so we do
  // not know whether or not it is still valid and the last constraint has a CommmitID that is up-to-date and is
  // therefore certainly valid.
  EXPECT_TRUE(key_constraint_is_confidently_valid(_table_b, {{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY}));
  EXPECT_FALSE(key_constraint_is_confidently_valid(_table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, CommitID{1}}));
  EXPECT_TRUE(key_constraint_is_confidently_valid(
      _table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, CommitID{2}, CommitID{1}}));
}

TEST_F(ConstraintUtilsTest, CheckIfTableKeyConstraintIsKnownToBeInvalid) {
  EXPECT_FALSE(key_constraint_is_confidently_invalid(
      _table_b, {{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY, MAX_COMMIT_ID, MAX_COMMIT_ID}));
  EXPECT_TRUE(key_constraint_is_confidently_invalid(
      _table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, CommitID{0}, CommitID{0}}));

  // Manually modify the `max_end_cid` of the chunk to simulate a delete to the table.
  _table_b->append({0, 1});
  _table_b->get_chunk(ChunkID{0})->mvcc_data()->max_end_cid = CommitID{2};

  // The first constraint is permanent and therefore NOT confidently invalid, the second was never verified but only
  // invalidated on a CommitID prior to the deletion so it is also NOT confidently invalid and the last constraint was
  // invalidated on a CommitID that is up-to-date and therefore certainly invalid.
  EXPECT_FALSE(key_constraint_is_confidently_invalid(_table_b, {{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY}));
  EXPECT_FALSE(key_constraint_is_confidently_invalid(
      _table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, MAX_COMMIT_ID, CommitID{1}}));
  EXPECT_TRUE(key_constraint_is_confidently_invalid(
      _table_b, {{ColumnID{1}}, KeyConstraintType::UNIQUE, CommitID{1}, CommitID{2}}));
}

}  // namespace hyrise
