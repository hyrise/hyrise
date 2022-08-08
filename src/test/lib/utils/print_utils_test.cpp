#include "base_test.hpp"

#include "storage/table.hpp"
#include "utils/print_utils.hpp"

namespace opossum {

class PrintUtilsTest : public BaseTest {};

TEST_F(PrintUtilsTest, table_key_constraints_to_stream) {
  const auto table = Table::create_dummy_table(
      {{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}});
  auto stream = std::ostringstream{};

  print_table_key_constraints(table, stream);
  // Empty constraints do not print anything.
  EXPECT_EQ(stream.str(), "");

  stream.str("");
  table->add_soft_key_constraint({{ColumnID{1}, ColumnID{0}}, KeyConstraintType::UNIQUE});
  table->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});

  print_table_key_constraints(table, stream);
  // Primary keys are printed before unique constraints.
  EXPECT_EQ(stream.str(), "PRIMARY_KEY(c), UNIQUE(a, b)");

  stream.str("");
  print_table_key_constraints(table, stream, " | ");
  // Separator is used.
  EXPECT_EQ(stream.str(), "PRIMARY_KEY(c) | UNIQUE(a, b)");
}

}  // namespace opossum
