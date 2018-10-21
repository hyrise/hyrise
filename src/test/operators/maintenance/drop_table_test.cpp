#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/maintenance/drop_table.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class DropTableTest : public BaseTest {
 public:
  void SetUp() override { drop_table = std::make_shared<DropTable>("t"); }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<DropTable> drop_table;
};

TEST_F(DropTableTest, NameAndDescription) {
  EXPECT_EQ(drop_table->name(), "Drop Table");
  EXPECT_EQ(drop_table->description(DescriptionMode::SingleLine), "Drop Table 't'");
  EXPECT_EQ(drop_table->description(DescriptionMode::MultiLine), "Drop Table 't'");
}

TEST_F(DropTableTest, Execute) {
  EXPECT_FALSE(StorageManager::get().has_table("t"));

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  StorageManager::get().add_table("t", table);

  EXPECT_TRUE(StorageManager::get().has_table("t"));

  drop_table->execute();

  EXPECT_FALSE(StorageManager::get().has_table("t"));
}

TEST_F(DropTableTest, NoSuchTable) { EXPECT_THROW(drop_table->execute(), std::logic_error); }

}  // namespace opossum
