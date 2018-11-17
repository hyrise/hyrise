#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class LoadTableTest : public BaseTest {};

TEST_F(LoadTableTest, EmptyTableFromHeader) {
  const auto tbl_header_table = create_table_from_header("src/test/tables/float_int.tbl");
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Float}, {"a", DataType::Int}}, TableType::Data);

  EXPECT_EQ(tbl_header_table->row_count(), 0);
  EXPECT_TABLE_EQ_UNORDERED(tbl_header_table, expected_table);
}

}  // namespace opossum
