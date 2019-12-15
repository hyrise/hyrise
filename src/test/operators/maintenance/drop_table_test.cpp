#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class DropTableTest : public BaseTest {
 public:
  void SetUp() override {
    drop_table = std::make_shared<DropTable>("t", false);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);

    table = std::make_shared<Table>(column_definitions, TableType::Data);
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<DropTable> drop_table;
};

TEST_F(DropTableTest, NameAndDescription) {
  EXPECT_EQ(drop_table->name(), "DropTable");
  EXPECT_EQ(drop_table->description(DescriptionMode::SingleLine), "DropTable 't'");
  EXPECT_EQ(drop_table->description(DescriptionMode::MultiLine), "DropTable 't'");
}

TEST_F(DropTableTest, Execute) {
  Hyrise::get().storage_manager.add_table("t", table);
  drop_table->execute();
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("t"));
}

TEST_F(DropTableTest, NoSuchTable) { EXPECT_THROW(drop_table->execute(), std::logic_error); }

TEST_F(DropTableTest, ExecuteWithIfExists) {
  Hyrise::get().storage_manager.add_table("t", table);
  auto drop_table_if_exists_1 = std::make_shared<DropTable>("t", true);
  drop_table_if_exists_1->execute();
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("t"));

  auto drop_table_if_exists_2 = std::make_shared<DropTable>("t", true);
  EXPECT_NO_THROW(drop_table_if_exists_2->execute());
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("t"));
}

}  // namespace opossum
