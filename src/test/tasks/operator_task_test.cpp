#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"

#include "operators/abstract_join_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class OperatorTaskTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _test_table_a);

    _test_table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _test_table_b);
  }

  std::shared_ptr<Table> _test_table_a, _test_table_b;
};

TEST_F(OperatorTaskTest, BasicTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  auto tasks = OperatorTask::make_tasks_from_operator(gt);

  auto result_task = tasks.back();
  result_task->schedule();

  EXPECT_TABLE_EQ(_test_table_a, result_task->get_operator()->get_output());
}

TEST_F(OperatorTaskTest, SingleDependencyTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  auto ts = std::make_shared<TableScan>(gt, ColumnID{0}, ScanType::OpEquals, 1234);

  auto tasks = OperatorTask::make_tasks_from_operator(ts);
  for (auto &task : tasks) {
    task->schedule();
  }

  auto expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);
  EXPECT_TABLE_EQ(expected_result, tasks.back()->get_operator()->get_output());
}

TEST_F(OperatorTaskTest, DoubleDependencyTasksFromOperatorTest) {
  auto gt_a = std::make_shared<GetTable>("table_a");
  auto gt_b = std::make_shared<GetTable>("table_b");
  auto join = std::make_shared<JoinHash>(gt_a, gt_b, JoinMode::Inner,
                                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals);

  auto tasks = OperatorTask::make_tasks_from_operator(join);
  for (auto &task : tasks) {
    task->schedule();
  }

  auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 2);
  EXPECT_TABLE_EQ(expected_result, tasks.back()->get_operator()->get_output());
}
}  // namespace opossum
