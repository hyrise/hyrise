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

TEST_F(OperatorTaskTest, BasicTasksFromOperatorArgsTest) {
  auto gt_task = OperatorTask::make_from_operator_args<GetTable>("table_a");
  gt_task->schedule();
  EXPECT_TABLE_EQ(_test_table_a, gt_task->get_operator()->get_output());
}

TEST_F(OperatorTaskTest, SingleDependencyTasksFromOperatorArgsTest) {
  auto gt_task = OperatorTask::make_from_operator_args<GetTable>("table_a");
  auto ts_task = OperatorTask::make_from_operator_args<TableScan>(gt_task, ColumnName("a"), "=", 1234);
  std::vector<std::shared_ptr<OperatorTask>> tasks = {gt_task, ts_task};

  for (auto &task : tasks) {
    task->schedule();
  }

  auto expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);
  EXPECT_TABLE_EQ(expected_result, tasks.back()->get_operator()->get_output());
}

TEST_F(OperatorTaskTest, DoubleDependencyTasksFromOperatorArgsTest) {
  auto gt_a_task = OperatorTask::make_from_operator_args<GetTable>("table_a");
  auto gt_b_task = OperatorTask::make_from_operator_args<GetTable>("table_b");
  auto join_task = OperatorTask::make_from_operator_args<JoinHash>(gt_a_task, gt_b_task,
                                                                   std::pair<std::string, std::string>("a", "a"), "=",
                                                                   Inner, std::string("left."), std::string("right."));

  std::vector<std::shared_ptr<OperatorTask>> tasks = {gt_a_task, gt_b_task, join_task};
  for (auto &task : tasks) {
    task->schedule();
  }

  auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 2);
  EXPECT_TABLE_EQ(expected_result, tasks.back()->get_operator()->get_output());
}
}  // namespace opossum
