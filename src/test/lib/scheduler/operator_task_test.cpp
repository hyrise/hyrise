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

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class OperatorTaskTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_a", _test_table_a);

    _test_table_b = load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_b", _test_table_b);
  }

  std::shared_ptr<Table> _test_table_a, _test_table_b;
};

TEST_F(OperatorTaskTest, BasicTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(gt);

  ASSERT_EQ(tasks.size(), 1);
  root_operator_task->schedule();

  EXPECT_TABLE_EQ_UNORDERED(_test_table_a, gt->get_output());
}

TEST_F(OperatorTaskTest, SingleDependencyTasksFromOperatorTest) {
  auto gt = std::make_shared<GetTable>("table_a");
  auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  auto ts = std::make_shared<TableScan>(gt, equals_(a, 1234));

  const auto& [tasks, _] = OperatorTask::make_tasks_from_operator(ts);
  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }

  auto expected_result = load_table("resources/test_data/tbl/int_float_filtered.tbl", ChunkOffset{2});
  EXPECT_TABLE_EQ_UNORDERED(expected_result, ts->get_output());
}

TEST_F(OperatorTaskTest, DoubleDependencyTasksFromOperatorTest) {
  auto gt_a = std::make_shared<GetTable>("table_a");
  auto gt_b = std::make_shared<GetTable>("table_b");
  auto join = std::make_shared<JoinHash>(
      gt_a, gt_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});

  const auto& [tasks, _] = OperatorTask::make_tasks_from_operator(join);
  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }

  auto expected_result = load_table("resources/test_data/tbl/join_operators/int_inner_join.tbl", ChunkOffset{2});
  EXPECT_TABLE_EQ_UNORDERED(expected_result, join->get_output());
}

TEST_F(OperatorTaskTest, MakeDiamondShape) {
  auto gt_a = std::make_shared<GetTable>("table_a");
  auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  auto b = PQPColumnExpression::from_table(*_test_table_a, "b");
  auto scan_a = std::make_shared<TableScan>(gt_a, greater_than_equals_(a, 1234));
  auto scan_b = std::make_shared<TableScan>(scan_a, less_than_(b, 1000));
  auto scan_c = std::make_shared<TableScan>(scan_a, greater_than_(b, 2000));
  auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);

  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(union_positions);

  ASSERT_EQ(tasks.size(), 5u);
  auto tasks_set = std::unordered_set<std::shared_ptr<AbstractTask>>(tasks.begin(), tasks.end());
  EXPECT_TRUE(tasks_set.contains(gt_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_b->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_c->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(union_positions->get_or_create_operator_task()));

  using TaskVector = std::vector<std::shared_ptr<AbstractTask>>;
  EXPECT_EQ(gt_a->get_or_create_operator_task()->successors(), TaskVector{scan_a->get_or_create_operator_task()});
  auto scan_a_successors = TaskVector{scan_b->get_or_create_operator_task(), scan_c->get_or_create_operator_task()};
  EXPECT_EQ(scan_a->get_or_create_operator_task()->successors(), scan_a_successors);
  EXPECT_EQ(scan_b->get_or_create_operator_task()->successors(),
            TaskVector{union_positions->get_or_create_operator_task()});
  EXPECT_EQ(scan_c->get_or_create_operator_task()->successors(),
            TaskVector{union_positions->get_or_create_operator_task()});
  EXPECT_EQ(union_positions->get_or_create_operator_task()->successors(), TaskVector{});

  for (auto& task : tasks) {
    task->schedule();
    // We don't have to wait here, because we are running the task tests without a scheduler
  }
}
}  // namespace hyrise
