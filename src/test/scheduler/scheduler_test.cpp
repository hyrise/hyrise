#include <memory>
#include <utility>
#include <vector>

#include "../base_test.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/processing_unit.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "utils/exception.hpp"

namespace opossum {

class SchedulerTest : public BaseTest {
 protected:
  void stress_linear_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    auto task2 = std::make_shared<JobTask>([&]() {
      auto current_value = 1u;
      auto successful = counter.compare_exchange_strong(current_value, 2u);
      ASSERT_TRUE(successful);
    });
    auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 2u;
      auto successful = counter.compare_exchange_strong(current_value, 3u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_multiple_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() { counter += 1u; });
    auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 3u;
      auto successful = counter.compare_exchange_strong(current_value, 4u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_diamond_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    auto task3 = std::make_shared<JobTask>([&]() { counter += 3u; });
    auto task4 = std::make_shared<JobTask>([&]() {
      auto current_value = 6u;
      auto successful = counter.compare_exchange_strong(current_value, 7u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task4);
    task3->set_as_predecessor_of(task4);

    task4->schedule();
    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void increment_counter_in_subtasks(std::atomic_uint& counter) {
    std::vector<std::shared_ptr<AbstractTask>> tasks;
    for (size_t i = 0; i < 10; i++) {
      auto task = std::make_shared<JobTask>([&]() {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        for (size_t j = 0; j < 3; j++) {
          auto job = std::make_shared<JobTask>([&]() { counter++; });

          job->schedule();
          jobs.emplace_back(job);
        }

        CurrentScheduler::wait_for_tasks(jobs);
      });
      task->schedule();
      tasks.emplace_back(task);
    }
  }
};

/**
 * Schedule some tasks with subtasks, make sure all of them finish
 */
TEST_F(SchedulerTest, BasicTest) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  std::atomic_uint counter{0};

  increment_counter_in_subtasks(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 30u);

  CurrentScheduler::set(nullptr);
}

TEST_F(SchedulerTest, BasicTestWithoutScheduler) {
  std::atomic_uint counter{0};
  increment_counter_in_subtasks(counter);
  ASSERT_EQ(counter, 30u);
}

TEST_F(SchedulerTest, LinearDependenciesWithScheduler) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  std::atomic_uint counter{0u};

  stress_linear_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, MultipleDependenciesWithScheduler) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  std::atomic_uint counter{0u};

  stress_multiple_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithScheduler) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  std::atomic_uint counter{0};

  stress_diamond_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, LinearDependenciesWithoutScheduler) {
  CurrentScheduler::set(nullptr);

  std::atomic_uint counter{0u};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, MultipleDependenciesWithoutScheduler) {
  CurrentScheduler::set(nullptr);

  std::atomic_uint counter{0u};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithoutScheduler) {
  CurrentScheduler::set(nullptr);

  std::atomic_uint counter{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, ExceptionInTaskWithScheduler) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(1, 1)));

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  for (auto i = 0u; i < ProcessingUnit::MAX_WORKERS_PER_CORE + 5; ++i) {
    auto job = std::make_shared<JobTask>([]() { throw InvalidInputException("Invalid Input!"); });
    job->schedule();
    jobs.emplace_back(job);
  }
  EXPECT_THROW(CurrentScheduler::wait_for_tasks(jobs), InvalidInputException);

  bool is_there_still_a_worker = false;
  jobs = {};
  auto job = std::make_shared<JobTask>([&]() { is_there_still_a_worker = true; });
  job->schedule();
  jobs.emplace_back(job);
  CurrentScheduler::wait_for_tasks(jobs);
  EXPECT_TRUE(is_there_still_a_worker);

  CurrentScheduler::get()->finish();
}

TEST_F(SchedulerTest, ExceptionInTaskWithoutScheduler) {
  CurrentScheduler::set(nullptr);

  // If we don't have a scheduler, the exception should not get caught.
  EXPECT_THROW(
      {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        for (auto i = 0u; i < 5; ++i) {
          auto job = std::make_shared<JobTask>([]() { throw InvalidInputException("Something went wrong!"); });
          job->schedule();
          jobs.emplace_back(job);
        }
      },
      InvalidInputException);
}

TEST_F(SchedulerTest, ExceptionInDependentTaskWithScheduler) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(1, 1)));

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  int step = 0;
  jobs.emplace_back(std::make_shared<JobTask>([&]() { step = 1; }));
  jobs.emplace_back(std::make_shared<JobTask>([&]() {
    step = 2;
    throw InvalidInputException("Boom");
  }));
  jobs.emplace_back(std::make_shared<JobTask>([&]() { step = 3; }));
  jobs[0]->set_as_predecessor_of(jobs[1]);
  jobs[1]->set_as_predecessor_of(jobs[2]);
  jobs[2]->schedule();
  jobs[1]->schedule();
  jobs[0]->schedule();

  EXPECT_THROW(CurrentScheduler::wait_for_tasks(jobs), InvalidInputException);
  EXPECT_EQ(step, 2);

  CurrentScheduler::get()->finish();
}

TEST_F(SchedulerTest, ExceptionInDependentTaskWithoutScheduler) {
  CurrentScheduler::set(nullptr);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  int step = 0;
  jobs.emplace_back(std::make_shared<JobTask>([&]() { step = 1; }));
  jobs.emplace_back(std::make_shared<JobTask>([&]() {
    step = 2;
    throw InvalidInputException("Boom");
  }));
  jobs.emplace_back(std::make_shared<JobTask>([&]() { step = 3; }));
  jobs[0]->set_as_predecessor_of(jobs[1]);
  jobs[1]->set_as_predecessor_of(jobs[2]);
  jobs[2]->schedule();
  jobs[1]->schedule();
  EXPECT_THROW(jobs[0]->schedule(), InvalidInputException);
  EXPECT_EQ(step, 2);
}

TEST_F(SchedulerTest, MultipleOperators) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  auto test_table = load_table("src/test/tables/int_float.tbl", 2);
  StorageManager::get().add_table("table", std::move(test_table));

  auto gt = std::make_shared<GetTable>("table");
  auto ts = std::make_shared<TableScan>(gt, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);

  auto gt_task = std::make_shared<OperatorTask>(gt);
  auto ts_task = std::make_shared<OperatorTask>(ts);
  gt_task->set_as_predecessor_of(ts_task);

  gt_task->schedule();
  ts_task->schedule();

  CurrentScheduler::get()->finish();

  auto expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);
  EXPECT_TABLE_EQ_UNORDERED(ts->get_output(), expected_result);
}

}  // namespace opossum
