#include <memory>

#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

TEST(operators_get_table, get_output_returns_correct_table) {
  auto t = std::make_shared<opossum::Table>(opossum::Table(2));
  opossum::StorageManager::get().add_table("aNiceTestTable", t);

  auto gt = std::make_shared<opossum::GetTable>("aNiceTestTable");
  gt->execute();

  EXPECT_EQ(gt->get_output(), t);
}

TEST(operators_get_table, get_output_throwns_on_unknown_table_name) {
  auto t = std::make_shared<opossum::Table>(opossum::Table(2));
  opossum::StorageManager::get().add_table("aNiceTestTable", t);

  auto gt = std::make_shared<opossum::GetTable>("anUglyTestTable");
  gt->execute();

  EXPECT_THROW(gt->get_output(), std::exception);
}
