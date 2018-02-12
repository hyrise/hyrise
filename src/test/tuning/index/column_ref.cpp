#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../../base_test.hpp"

#include "storage/table.hpp"
#include "tuning/index/column_ref.hpp"

namespace opossum {

class ColumnRefTest : public BaseTest {};

TEST_F(ColumnRefTest, GetPropertiesTest) {
  std::vector<ColumnID> column_ids{ColumnID{123}};
  ColumnRef column_ref{"table_name", column_ids};

  EXPECT_EQ(column_ref.table_name, "table_name");
  EXPECT_EQ(column_ref.column_ids, column_ids);
}

TEST_F(ColumnRefTest, GreaterThanOperatorTest) {
  std::vector<ColumnID> column_ids_smaller{ColumnID{123}, ColumnID{456}};
  std::vector<ColumnID> column_ids_bigger{ColumnID{789}, ColumnID{901}};

  ColumnRef cref_smaller{"table_name", column_ids_smaller};
  ColumnRef cref_bigger{"table_name", column_ids_bigger};
  ColumnRef cref_other_table{"other_table", column_ids_smaller};

  EXPECT_FALSE(cref_smaller > cref_smaller);

  EXPECT_TRUE(cref_smaller > cref_other_table);
  EXPECT_TRUE(cref_bigger > cref_smaller);
}

TEST_F(ColumnRefTest, LessThanOperatorTest) {
  std::vector<ColumnID> column_ids_smaller{ColumnID{123}, ColumnID{456}};
  std::vector<ColumnID> column_ids_bigger{ColumnID{789}, ColumnID{901}};

  ColumnRef cref_smaller{"table_name", column_ids_smaller};
  ColumnRef cref_bigger{"table_name", column_ids_bigger};
  ColumnRef cref_other_table{"other_table", column_ids_smaller};

  EXPECT_FALSE(cref_smaller < cref_smaller);
  EXPECT_FALSE(cref_smaller < cref_other_table);

  EXPECT_TRUE(cref_smaller < cref_bigger);
}

TEST_F(ColumnRefTest, StreamingOperatorTest) {
  std::vector<ColumnID> column_ids{ColumnID{0}};
  ColumnRef column_ref{"table_name", column_ids};

  auto table = std::make_shared<Table>();
  table->add_column("column_name", DataType::Int);
  StorageManager::get().add_table("table_name", table);

  std::stringstream stream;
  stream << column_ref;

  std::string result = stream.str();
  EXPECT_EQ(result, "table_name.(column_name)");
}
}  // namespace opossum
