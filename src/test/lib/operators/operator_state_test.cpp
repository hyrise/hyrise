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

class OperatorSharedStateTest : public BaseTest {
 public:
  constexpr static auto JOB_COUNT = size_t{100};
};

class TestWorkerState : public WorkerLocalState<TestWorkerState> {
 public:
  virtual void merge(TestWorkerState& other) final {
    task_count += other.task_count;
  }

  size_t task_count{0};
};

TEST_F(OperatorSharedStateTest, ExecuteSingleThreaded) {
  EXPECT_FALSE(Hyrise::get().is_multi_threaded());

  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(JOB_COUNT);

  for (auto job_id = size_t{0}; job_id < JOB_COUNT; ++job_id) {
    jobs[job_id] = std::make_shared<JobTask>([&] {
      auto& worker_state = operator_state.current_worker_state();
      ++worker_state.task_count;
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, JOB_COUNT);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, JOB_COUNT);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, JOB_COUNT);
}

TEST_F(OperatorSharedStateTest, MoreTasksThanWorkers) {
  const auto core_count = std::min(Hyrise::get().topology.num_cpus(), JOB_COUNT / 10);

  if (core_count < 2) {
    GTEST_SKIP();
  }

  Hyrise::get().topology.use_default_topology(core_count);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  EXPECT_TRUE(Hyrise::get().is_multi_threaded());

  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(JOB_COUNT);

  for (auto job_id = size_t{0}; job_id < JOB_COUNT; ++job_id) {
    jobs[job_id] = std::make_shared<JobTask>([&] {
      auto& worker_state = operator_state.current_worker_state();
      ++worker_state.task_count;
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto worker_states = operator_state.worker_states();
  EXPECT_LE(worker_states.size(), core_count);
  EXPECT_LE(worker_states.front().get().task_count, JOB_COUNT);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, JOB_COUNT);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, JOB_COUNT);
}

TEST_F(OperatorSharedStateTest, MoreWorkersThanTasks) {
  constexpr auto TASK_COUNT = 4;

  if (Hyrise::get().topology.num_cpus() <= TASK_COUNT) {
    GTEST_SKIP();
  }
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  EXPECT_TRUE(Hyrise::get().is_multi_threaded());

  auto operator_state = OperatorSharedState<TestWorkerState>{};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(TASK_COUNT);

  for (auto job_id = size_t{0}; job_id < TASK_COUNT; ++job_id) {
    jobs[job_id] = std::make_shared<JobTask>([&] {
      auto& worker_state = operator_state.current_worker_state();
      ++worker_state.task_count;
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto worker_states = operator_state.worker_states();
  EXPECT_LE(worker_states.size(), TASK_COUNT);
  EXPECT_LE(worker_states.front().get().task_count, TASK_COUNT);

  const auto& merged_state = operator_state.merge_worker_states();
  EXPECT_EQ(merged_state.task_count, TASK_COUNT);

  worker_states = operator_state.worker_states();
  EXPECT_EQ(worker_states.size(), 1);
  EXPECT_EQ(worker_states.front().get().task_count, TASK_COUNT);
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

}  // namespace hyrise
