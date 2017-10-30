#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>();
    auto t2 = std::make_shared<Table>(4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);

    auto v1 = std::make_shared<StoredTableNode>("first_table");
    auto v2 = std::make_shared<StoredTableNode>("second_table");

    sm.add_view("first_view", std::move(v1));
    sm.add_view("second_view", std::move(v2));
  }
};

TEST_F(StorageManagerTest, AddTableTwice) {
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.add_table("first_table", std::make_shared<Table>()), std::exception);
  EXPECT_THROW(sm.add_table("first_view", std::make_shared<Table>()), std::exception);
}

TEST_F(StorageManagerTest, GetTable) {
  auto& sm = StorageManager::get();
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
}

TEST_F(StorageManagerTest, DropTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}

TEST_F(StorageManagerTest, ResetTable) {
  StorageManager::reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageManagerTest, HasTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageManagerTest, AddViewTwice) {
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.add_view("first_table", std::make_shared<StoredTableNode>("first_table")), std::exception);
  EXPECT_THROW(sm.add_view("first_view", std::make_shared<StoredTableNode>("first_table")), std::exception);
}

TEST_F(StorageManagerTest, GetView) {
  auto& sm = StorageManager::get();
  auto v3 = sm.get_view("first_view");
  auto v4 = sm.get_view("second_view");
  EXPECT_THROW(sm.get_view("third_view"), std::exception);
}

TEST_F(StorageManagerTest, DropView) {
  auto& sm = StorageManager::get();
  sm.drop_view("first_view");
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
  EXPECT_THROW(sm.drop_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, ResetView) {
  StorageManager::reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveView) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_view("third_view"), false);
}

TEST_F(StorageManagerTest, HasView) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_view("first_view"), true);
}

}  // namespace opossum
