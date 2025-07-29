#include "base_test.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"

namespace hyrise {

class TaskQueueTest : public BaseTest {
 public:
  static bool try_transition_to_scheduled(std::shared_ptr<JobTask>& task) {
    return task->_try_transition_to(TaskState::Scheduled);
  }
};

TEST_F(TaskQueueTest, StealableJobs) {
  auto task_queue = TaskQueue{NodeID{0}};

  auto task_1 = std::make_shared<JobTask>([]() {}, SchedulePriority::High, true);
  EXPECT_TRUE(try_transition_to_scheduled(task_1));
  task_queue.push(task_1, SchedulePriority::High);
  EXPECT_FALSE(task_queue.empty());
  // Stealable job can be stolen.
  EXPECT_EQ(task_queue.steal(), task_1);
  EXPECT_TRUE(task_queue.empty());

  auto task_2 = std::make_shared<JobTask>([]() {}, SchedulePriority::High, false);
  EXPECT_TRUE(try_transition_to_scheduled(task_2));
  task_queue.push(task_2, SchedulePriority::High);
  EXPECT_FALSE(task_queue.empty());
  // Unstealable job cannot be stolen.
  EXPECT_EQ(task_queue.steal(), nullptr);
  EXPECT_FALSE(task_queue.empty());
}

TEST_F(TaskQueueTest, EstimateLoad) {
  auto task_queue = TaskQueue{NodeID{0}};

  auto task_1 = std::make_shared<JobTask>([]() {}, SchedulePriority::High);
  auto task_2 = std::make_shared<JobTask>([]() {}, SchedulePriority::Default);

  EXPECT_TRUE(task_queue.empty());
  // Task that has not yet been scheduled, will not be added to the task queue. Pushing tries to mark the task as
  // enqueued, which cannot be done to tasks that are not scheduled yet.
  EXPECT_THROW(task_queue.push(task_1, SchedulePriority::High), std::logic_error);
  EXPECT_EQ(task_1->state(), TaskState::Created);
  EXPECT_TRUE(task_queue.empty());

  EXPECT_TRUE(try_transition_to_scheduled(task_1));
  EXPECT_EQ(task_1->state(), TaskState::Scheduled);
  EXPECT_TRUE(try_transition_to_scheduled(task_2));

  EXPECT_TRUE(task_queue.empty());
  task_queue.push(task_1, SchedulePriority::High);
  task_queue.push(task_2, SchedulePriority::Default);
  // Tasks of higher priority are weighted higher. Tasks with the default priority have a multiplier of 1, while high
  // priority tasks have a multiplier of two. Thus we calculate the load as `1 * 2^0 + 1 * 2^1`.
  EXPECT_EQ(task_queue.estimate_load(), size_t{3});
}

}  // namespace hyrise
