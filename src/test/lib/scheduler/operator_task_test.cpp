#include <future>
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
#include "scheduler/node_queue_scheduler.hpp"
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

TEST_F(OperatorTaskTest, ConcurrentTaskReusage) {
  // Operators reuse the created OperatorTasks when the tasks are still available. This may happen concurrently, e.g.,
  // when an uncorrelated subquery is used in multiple TableScans. This test ensures that concurrently creating/reusing
  // tasks from the same operator is thread-safe and does not lead to segmentation faults or tasks waiting forever to
  // finish.
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto get_table = std::make_shared<GetTable>("table_b");
  const auto scan_predicate = greater_than_(pqp_column_(ColumnID{0}, DataType::Int, false, "a"), 100);
  const auto table_scan = std::make_shared<TableScan>(get_table, scan_predicate);

  const auto num_threads = 50;
  const auto iterations_per_thread = 50;
  auto thread_futures = std::vector<std::future<void>>{};
  thread_futures.reserve(num_threads);
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);
  auto successful_executions = std::atomic_size_t{0};

  for (auto thread_id = 0; thread_id < num_threads; ++thread_id) {
    auto task = std::packaged_task<void()>{[&]() {
      for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
        const auto& [tasks, _] = OperatorTask::make_tasks_from_operator(table_scan);
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
        if (table_scan->get_output()) {
          ++successful_executions;
        }
      }
    }};

    thread_futures.emplace_back(task.get_future());
    threads.emplace_back(std::move(task));
  }

  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(180)) == std::future_status::timeout) {
      FAIL() << "At least one thread got stuck and did not commit.";
    }
    // Retrieve the future so that exceptions stored in its state are thrown
    thread_future.get();
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(successful_executions, num_threads * iterations_per_thread);
}

}  // namespace hyrise
