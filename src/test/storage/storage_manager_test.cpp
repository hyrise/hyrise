#include <memory>

#include "gtest/gtest.h"

#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

class StorageStorageManagerTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    auto &sm = opossum::StorageManager::get();
    auto t1 = std::make_shared<opossum::Table>();
    auto t2 = std::make_shared<opossum::Table>(4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto &sm = opossum::StorageManager::get();
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto &sm = opossum::StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  // TODO(MB): Should the StorageManager catch exceptions for get_table executed with unknown name?
  // EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}
