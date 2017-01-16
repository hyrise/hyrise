#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/nested_loop_join.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsNestedLoopJoinTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table_left = load_table("src/test/tables/nlj_left.tbl", 2);
    StorageManager::get().add_table("table_left", std::move(test_table_left));
    std::shared_ptr<Table> test_table_right = load_table("src/test/tables/nlj_right.tbl", 2);
    StorageManager::get().add_table("table_right", std::move(test_table_right));
  }
};

TEST_F(OperatorsNestedLoopJoinTest, ValueJoinValue) {
  auto gt_left = std::make_shared<GetTable>("table_left");
  gt_left->execute();
  auto gt_right = std::make_shared<GetTable>("table_right");
  gt_right->execute();
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/nlj_result.tbl", 1);
  auto join_operator = std::make_shared<NestedLoopJoin>(gt_left, gt_right, "left_c3", "right_c3", "=", JoinMode::Inner);
  join_operator->execute();

  EXPECT_TABLE_EQ(join_operator->get_output(), expected_result);
}

}  // namespace opossum
