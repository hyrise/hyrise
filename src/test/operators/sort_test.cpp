#include <iostream>
#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../common.hpp"

namespace opossum {

class OperatorsSortTest : public ::testing::Test {
  virtual void SetUp() {
    std::shared_ptr<opossum::Table> test_table = opossum::loadTable("src/test/tables/int_float.tbl", 2);
    opossum::StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<opossum::GetTable>("table_a");
  }

  virtual void TearDown() { opossum::StorageManager::get().drop_table("table_a"); }

 public:
  std::shared_ptr<opossum::GetTable> _gt;
};

TEST_F(OperatorsSortTest, AscendingSortOfOneColumn) {
  std::shared_ptr<opossum::Table> expected_result = opossum::loadTable("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<opossum::Sort>(_gt, "a");
  sort->execute();

  EXPECT_TRUE(tablesEqual(*(sort->get_output()), *expected_result, true));
}

TEST_F(OperatorsSortTest, DISABLED_DoubleSortOfOneColumn) {
  std::shared_ptr<opossum::Table> expected_result = opossum::loadTable("src/test/tables/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<opossum::Sort>(_gt, "a", false);
  sort1->execute();

  auto sort2 = std::make_shared<opossum::Sort>(sort1, "a");
  sort2->execute();

  EXPECT_TRUE(tablesEqual(*(sort2->get_output()), *expected_result, true));
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumn) {
  std::shared_ptr<opossum::Table> expected_result = opossum::loadTable("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<opossum::Sort>(_gt, "a", false);
  sort->execute();

  EXPECT_TRUE(tablesEqual(*(sort->get_output()), *expected_result, true));
}

TEST_F(OperatorsSortTest, DISABLED_MultipleColumnSort) {
  std::shared_ptr<opossum::Table> test_table = opossum::loadTable("src/test/tables/int_float2.tbl", 2);
  opossum::StorageManager::get().add_table("test_table_sort_b", std::move(test_table));
  auto gt = std::make_shared<opossum::GetTable>("test_table_sort_b");

  std::shared_ptr<opossum::Table> expected_result = opossum::loadTable("src/test/tables/int_float2_sorted.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.

  auto sort_after_b = std::make_shared<opossum::Sort>(gt, "b");
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<opossum::Sort>(sort_after_b, "a");
  sort_after_a->execute();

  EXPECT_TRUE(tablesEqual(*(sort_after_a->get_output()), *expected_result, true));
}

}  // namespace opossum
