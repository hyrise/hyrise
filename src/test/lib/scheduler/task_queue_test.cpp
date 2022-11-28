#include "base_test.hpp"

#include "scheduler/job_task.hpp"
#include "scheduler/task_queue.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class TaskQueueTest : public BaseTest {};

TEST_F(TaskQueueTest, StealableJobs) {
  auto task_queue = TaskQueue{NodeID{0}};

  task_queue.push(std::make_shared<JobTask>([]() { return; }, SchedulePriority::High, true), SchedulePriority::High);
  EXPECT_TRUE(task_queue.steal());

  task_queue.push(std::make_shared<JobTask>([]() { return; }, SchedulePriority::High, false), SchedulePriority::High);
  EXPECT_FALSE(task_queue.steal());
}

TEST_F(TaskQueueTest, EstimateLoad) {
  auto task_queue = TaskQueue{NodeID{0}};

  task_queue.push(std::make_shared<JobTask>([]() { return; }, SchedulePriority::High), SchedulePriority::High);
  task_queue.push(std::make_shared<JobTask>([]() { return; }, SchedulePriority::Default), SchedulePriority::Default);

  EXPECT_EQ(task_queue.estimate_load(), size_t{3});
}

}  // namespace hyrise
