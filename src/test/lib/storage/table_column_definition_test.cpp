#include <memory>

#include "base_test.hpp"

#include "storage/table_column_definition.hpp"
#include "storage/table_key_constraint.hpp"

namespace opossum {

class TableColumnDefinitionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(TableColumnDefinitionTest, HashingAndEqualityCheck) {
  TableColumnDefinition column_definition{"a", DataType::Int, false};
  TableColumnDefinition equal_column_definition{"a", DataType::Int, false};
  TableColumnDefinition different_column_definition_a{"c", DataType::Int, false};
  TableColumnDefinition different_column_definition_b{"a", DataType::Double, false};
  TableColumnDefinition different_column_definition_c{"a", DataType::Int, true};
  TableColumnDefinition different_column_definition_d{"a", DataType::Int, false, new std::vector<hsql::ConstraintType>({hsql::ConstraintType::PRIMARY_KEY})};

  EXPECT_EQ(column_definition, equal_column_definition);
  // `operator!=` is not implemented for TableColumnDefinition,
  // therefore EXPECT_FALSE is used instead of EXPECT_NE
  EXPECT_FALSE(column_definition == different_column_definition_a);
  EXPECT_FALSE(column_definition == different_column_definition_b);
  EXPECT_FALSE(column_definition == different_column_definition_c);
  EXPECT_FALSE(column_definition == different_column_definition_d);

  EXPECT_EQ(column_definition.hash(), equal_column_definition.hash());
  EXPECT_NE(column_definition.hash(), different_column_definition_a.hash());
  EXPECT_NE(column_definition.hash(), different_column_definition_b.hash());
  EXPECT_NE(column_definition.hash(), different_column_definition_c.hash());
  EXPECT_NE(column_definition.hash(), different_column_definition_d.hash());
}

}  // namespace opossum
