#include "base_test.hpp"
#include "gtest/gtest.h"
#include "sql/sql_identifier.hpp"

namespace opossum {

class SQLIdentifierTest : public BaseTest {};

TEST_F(SQLIdentifierTest, StdHashOverload) {
  {
    const std::hash<SQLIdentifier> hasher;

    const auto column_name_0 = SQLIdentifier("col_1", {});
    const auto column_name_1 = SQLIdentifier("col_1", {});

    const auto column_table_name_0 = SQLIdentifier("col_1", "table_17");
    const auto column_table_name_1 = SQLIdentifier("col_1", "table_17");

    EXPECT_EQ(hasher(column_name_0), hasher(column_name_1));
    EXPECT_EQ(std::hash<SQLIdentifier>{}(column_table_name_0), std::hash<SQLIdentifier>{}(column_table_name_1));
    EXPECT_NE(hasher(column_name_0), hasher(column_table_name_0));
  }
}

}  // namespace opossum