#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/operator_state.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "types.hpp"

namespace hyrise {

class OperatorSharedStateTest : public BaseTest {};

namespace {

constexpr static auto TASK_COUNT_HIGH = size_t{100};
constexpr static auto TASK_COUNT_LOW = size_t{4};

class TestWorkerState : public AbstractWorkerState<TestWorkerState> {
 public:
  virtual void merge(TestWorkerState& other) final {
    task_count += other.task_count;
  }

  size_t task_count{0};
};

}  // namespace

TEST_F(OperatorSharedStateTest, ExecuteSingleThreaded) {
  EXPECT_FALSE(Hyrise::get().is_multi_threaded());
  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(TASK_COUNT_HIGH);

  for (auto job_id = size_t{0}; job_id < TASK_COUNT_HIGH; ++job_id) {
    jobs[job_id] = std::make_shared<JobTask>([&] {
      ++operator_state.current_worker_state().task_count;
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_HIGH);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, TASK_COUNT_HIGH);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_HIGH);
}

TEST_F(OperatorSharedStateTest, MoreTasksThanWorkers) {
  const auto core_count = std::min(Hyrise::get().topology.num_cpus(), TASK_COUNT_HIGH / 10);

  if (core_count < 2) {
    GTEST_SKIP();
  }

  Hyrise::get().topology.use_default_topology(core_count);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto start_flag = std::atomic_flag{};

  for (auto job_id = size_t{0}; job_id < TASK_COUNT_HIGH; ++job_id) {
    std::make_shared<JobTask>([&] {
      start_flag.wait(false);
      ++operator_state.current_worker_state().task_count;
    })->schedule();
  }

  start_flag.test_and_set();
  start_flag.notify_all();
  Hyrise::get().scheduler()->wait_for_all_tasks();

  auto worker_states = operator_state.worker_states();
  EXPECT_LE(worker_states.size(), core_count);
  EXPECT_LE(worker_states.front().get().task_count, TASK_COUNT_HIGH);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, TASK_COUNT_HIGH);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_HIGH);
}

TEST_F(OperatorSharedStateTest, MoreWorkersThanTasks) {
  if (Hyrise::get().topology.num_cpus() <= TASK_COUNT_LOW) {
    GTEST_SKIP();
  }

  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto start_flag = std::atomic_flag{};

  for (auto job_id = size_t{0}; job_id < TASK_COUNT_LOW; ++job_id) {
    std::make_shared<JobTask>([&] {
      ++operator_state.current_worker_state().task_count;
    })->schedule();
  }

  start_flag.test_and_set();
  start_flag.notify_all();
  Hyrise::get().scheduler()->wait_for_all_tasks();

  auto worker_states = operator_state.worker_states();
  EXPECT_LE(worker_states.size(), TASK_COUNT_LOW);
  EXPECT_LE(worker_states.front().get().task_count, TASK_COUNT_LOW);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, TASK_COUNT_LOW);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_LOW);
}

TEST_F(OperatorSharedStateTest, NoWorkDone) {
  auto operator_state = OperatorSharedState<TestWorkerState>{};

  EXPECT_THROW(operator_state.worker_states(), std::logic_error);
  EXPECT_THROW(operator_state.merge_worker_states(), std::logic_error);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  EXPECT_THROW(operator_state.worker_states(), std::logic_error);
  EXPECT_THROW(operator_state.merge_worker_states(), std::logic_error);
}

TEST_F(OperatorSharedStateTest, AccessWithoutWorker) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  auto operator_state = OperatorSharedState<TestWorkerState>{};

  for (auto job_id = size_t{0}; job_id < TASK_COUNT_HIGH; ++job_id) {
    ++operator_state.current_worker_state().task_count;
  }

  auto worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_HIGH);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, TASK_COUNT_HIGH);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT_HIGH);
}

}  // namespace hyrise
