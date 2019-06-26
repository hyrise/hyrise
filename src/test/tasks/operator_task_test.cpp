#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"
#include "scheduler/operator_task.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorTaskTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", _test_table_a);

    _test_table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_b", _test_table_b);
  }

  std::shared_ptr<Table> _test_table_a, _test_table_b;
};

TEST_F(OperatorTaskTest, BasicTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  auto tasks = OperatorTask::make_tasks_from_operator(gt, CleanupTemporaries::Yes);

  auto result_task = tasks.back();
  result_task->schedule();

  EXPECT_TABLE_EQ_UNORDERED(_test_table_a, result_task->get_operator()->get_output());
}

TEST_F(OperatorTaskTest, SingleDependencyTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  auto ts = std::make_shared<TableScan>(gt, equals_(a, 1234));

  auto tasks = OperatorTask::make_tasks_from_operator(ts, CleanupTemporaries::Yes);
  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }

  auto expected_result = load_table("resources/test_data/tbl/int_float_filtered.tbl", 2);
  EXPECT_TABLE_EQ_UNORDERED(expected_result, tasks.back()->get_operator()->get_output());

  // Check that everything was properly cleaned up
  EXPECT_EQ(gt->get_output(), nullptr);
}

TEST_F(OperatorTaskTest, DoubleDependencyTasksFromOperatorTest) {
  auto gt_a = std::make_shared<GetTable>("table_a");
  auto gt_b = std::make_shared<GetTable>("table_b");
  auto join = std::make_shared<JoinHash>(
      gt_a, gt_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});

  auto tasks = OperatorTask::make_tasks_from_operator(join, CleanupTemporaries::Yes);
  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }

  auto expected_result = load_table("resources/test_data/tbl/join_operators/int_inner_join.tbl", 2);
  EXPECT_TABLE_EQ_UNORDERED(expected_result, tasks.back()->get_operator()->get_output());

  // Check that everything was properly cleaned up
  EXPECT_EQ(gt_a->get_output(), nullptr);
  EXPECT_EQ(gt_b->get_output(), nullptr);
}

TEST_F(OperatorTaskTest, MakeDiamondShape) {
  auto gt_a = std::make_shared<GetTable>("table_a");
  auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  auto b = PQPColumnExpression::from_table(*_test_table_a, "b");
  auto scan_a = std::make_shared<TableScan>(gt_a, greater_than_equals_(a, 1234));
  auto scan_b = std::make_shared<TableScan>(scan_a, less_than_(b, 1000));
  auto scan_c = std::make_shared<TableScan>(scan_a, greater_than_(b, 2000));
  auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);

  auto tasks = OperatorTask::make_tasks_from_operator(union_positions, CleanupTemporaries::Yes);

  ASSERT_EQ(tasks.size(), 5u);
  EXPECT_EQ(tasks[0]->get_operator(), gt_a);
  EXPECT_EQ(tasks[1]->get_operator(), scan_a);
  EXPECT_EQ(tasks[2]->get_operator(), scan_b);
  EXPECT_EQ(tasks[3]->get_operator(), scan_c);
  EXPECT_EQ(tasks[4]->get_operator(), union_positions);

  std::vector<std::shared_ptr<AbstractTask>> expected_successors_0({tasks[1]});
  EXPECT_EQ(tasks[0]->successors(), expected_successors_0);

  std::vector<std::shared_ptr<AbstractTask>> expected_successors_1({tasks[2], tasks[3]});
  EXPECT_EQ(tasks[1]->successors(), expected_successors_1);

  std::vector<std::shared_ptr<AbstractTask>> expected_successors_2({tasks[4]});
  EXPECT_EQ(tasks[2]->successors(), expected_successors_2);

  std::vector<std::shared_ptr<AbstractTask>> expected_successors_3({tasks[4]});
  EXPECT_EQ(tasks[3]->successors(), expected_successors_3);

  std::vector<std::shared_ptr<AbstractTask>> expected_successors_4{};
  EXPECT_EQ(tasks[4]->successors(), expected_successors_4);

  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }

  // Check that everything was properly cleaned up
  EXPECT_EQ(gt_a->get_output(), nullptr);
  EXPECT_EQ(scan_a->get_output(), nullptr);
  EXPECT_EQ(scan_b->get_output(), nullptr);
  EXPECT_EQ(scan_c->get_output(), nullptr);
}
}  // namespace opossum
