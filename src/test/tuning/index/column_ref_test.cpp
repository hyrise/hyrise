#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../../base_test.hpp"

#include "storage/table.hpp"
#include "tuning/index/indexable_column_set.hpp"

namespace opossum {

class IndexableColumnSetTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_name", DataType::Int);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    StorageManager::get().add_table("table_name", table);
  }

  void TearDown() override { StorageManager::get().drop_table("table_name"); }
};

TEST_F(IndexableColumnSetTest, GetProperties) {
  std::vector<ColumnID> column_ids{ColumnID{123}};
  IndexableColumnSet indexable_column_set{"table_name", column_ids};

  EXPECT_EQ(indexable_column_set.table_name, "table_name");
  EXPECT_EQ(indexable_column_set.column_ids, column_ids);
}

TEST_F(IndexableColumnSetTest, LessThanOperator) {
  std::vector<ColumnID> column_ids_smaller{ColumnID{123}, ColumnID{456}};
  std::vector<ColumnID> column_ids_bigger{ColumnID{789}, ColumnID{901}};

  IndexableColumnSet cref_smaller{"table_name", column_ids_smaller};
  IndexableColumnSet cref_bigger{"table_name", column_ids_bigger};
  IndexableColumnSet cref_other_table{"z_other_table", column_ids_smaller};

  EXPECT_LT(cref_smaller, cref_other_table);
  EXPECT_LT(cref_smaller, cref_bigger);

  EXPECT_FALSE(cref_other_table < cref_smaller);
  EXPECT_FALSE(cref_bigger < cref_smaller);
}

TEST_F(IndexableColumnSetTest, StreamingOperator) {
  std::vector<ColumnID> column_ids{ColumnID{0}};
  IndexableColumnSet indexable_column_set{"table_name", column_ids};

  std::stringstream stream;
  stream << indexable_column_set;

  std::string result = stream.str();
  EXPECT_EQ(result, "table_name.(column_name)");
}

TEST_F(IndexableColumnSetTest, Equality) {
  std::vector<ColumnID> column_ids{ColumnID{0}};
  IndexableColumnSet indexable_column_set{"table_name", column_ids};

  std::vector<ColumnID> same_column_ids{ColumnID{0}};
  IndexableColumnSet indexable_column_set_same_column_ids{"table_name", same_column_ids};

  std::vector<ColumnID> different_column_ids{ColumnID{1}};
  IndexableColumnSet indexable_column_set_different_column_ids{"table_name", different_column_ids};

  IndexableColumnSet indexable_column_set_other_table{"other_table", same_column_ids};

  EXPECT_EQ(indexable_column_set, indexable_column_set);
  EXPECT_EQ(indexable_column_set, indexable_column_set_same_column_ids);

  EXPECT_FALSE(indexable_column_set == indexable_column_set_other_table);
  EXPECT_FALSE(indexable_column_set == indexable_column_set_different_column_ids);
}
}  // namespace opossum
