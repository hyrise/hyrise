#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/maintenance/create_table.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class CreateTableTest : public BaseTest {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);

    create_table = std::make_shared<CreateTable>("t", column_definitions, false);
  }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<CreateTable> create_table;
};

TEST_F(CreateTableTest, NameAndDescription) {
  EXPECT_EQ(create_table->name(), "Create Table");
  EXPECT_EQ(create_table->description(DescriptionMode::SingleLine),
            "Create Table 't' ('a' int NOT NULL, 'b' float NULL)");
  EXPECT_EQ(create_table->description(DescriptionMode::MultiLine),
            "Create Table 't' ('a' int NOT NULL\n'b' float NULL)");
}

TEST_F(CreateTableTest, Execute) {
  create_table->execute();

  EXPECT_TRUE(StorageManager::get().has_table("t"));

  const auto table = StorageManager::get().get_table("t");

  EXPECT_EQ(table->row_count(), 0);
  EXPECT_EQ(table->column_definitions(), column_definitions);
}

TEST_F(CreateTableTest, TableAlreadyExists) {
  create_table->execute();  // Table name "t" is taken now

  const auto create_different_table = std::make_shared<CreateTable>("t2", column_definitions, false);
  const auto create_same_table = std::make_shared<CreateTable>("t", column_definitions, false);

  EXPECT_NO_THROW(create_different_table->execute());
  EXPECT_THROW(create_same_table->execute(), std::logic_error);
}

TEST_F(CreateTableTest, ExecuteWithIfNotExists) {
  const auto ct_if_not_exists_1 = std::make_shared<CreateTable>("t", column_definitions, true);
  ct_if_not_exists_1->execute();

  EXPECT_TRUE(StorageManager::get().has_table("t"));

  const auto table = StorageManager::get().get_table("t");

  EXPECT_EQ(table->row_count(), 0);
  EXPECT_EQ(table->column_definitions(), column_definitions);

  const auto ct_if_not_exists_2 = std::make_shared<CreateTable>("t", column_definitions, true);
  EXPECT_NO_THROW(ct_if_not_exists_2->execute());
}
}  // namespace opossum
