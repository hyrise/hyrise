#include "base_test.hpp"

#include "scheduler/task_utils.hpp"

namespace hyrise {

class TaskUtilsTest : public BaseTest {
 public:
  class MockTask : public AbstractTask {
   public:
    MockTask() : AbstractTask{TaskType::MockTask} {}

   protected:
    void _on_execute() final {}
  };

  void SetUp() override {
    _task_a = std::make_shared<MockTask>();
    _task_b = std::make_shared<MockTask>();
    _task_c = std::make_shared<MockTask>();
    _task_d = std::make_shared<MockTask>();
  }

  std::shared_ptr<AbstractTask> _task_a;
  std::shared_ptr<AbstractTask> _task_b;
  std::shared_ptr<AbstractTask> _task_c;
  std::shared_ptr<AbstractTask> _task_d;
};

TEST_F(TaskUtilsTest, VisitTasksStreamlineTasks) {
  _task_a->set_as_predecessor_of(_task_b);
  _task_b->set_as_predecessor_of(_task_c);
  _task_c->set_as_predecessor_of(_task_d);

  auto expected_tasks = std::vector<std::shared_ptr<AbstractTask>>{_task_d, _task_c, _task_b, _task_a};
  auto actual_tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  visit_tasks(_task_d, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskVisitation::VisitPredecessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);

  actual_tasks.clear();
  std::reverse(expected_tasks.begin(), expected_tasks.end());

  visit_tasks_upwards(_task_a, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskUpwardVisitation::VisitSuccessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);
}

TEST_F(TaskUtilsTest, VisitTasksDiamondStructure) {
  _task_a->set_as_predecessor_of(_task_b);
  _task_a->set_as_predecessor_of(_task_c);
  _task_b->set_as_predecessor_of(_task_d);
  _task_c->set_as_predecessor_of(_task_d);

  auto expected_tasks = std::vector<std::shared_ptr<AbstractTask>>{_task_d, _task_b, _task_c, _task_a};
  auto actual_tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  visit_tasks(_task_d, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskVisitation::VisitPredecessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);

  actual_tasks.clear();
  expected_tasks = {_task_a, _task_b, _task_c, _task_d};

  visit_tasks_upwards(_task_a, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskUpwardVisitation::VisitSuccessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);
}

TEST_F(TaskUtilsTest, VisitTasksNonConstTasks) {
  _task_a->set_as_predecessor_of(_task_b);

  auto expected_tasks = std::vector<std::shared_ptr<AbstractTask>>{_task_b, _task_a};
  auto actual_tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  visit_tasks(_task_b, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskVisitation::VisitPredecessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);

  actual_tasks.clear();
  std::reverse(expected_tasks.begin(), expected_tasks.end());

  visit_tasks_upwards(_task_a, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskUpwardVisitation::VisitSuccessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);
}

TEST_F(TaskUtilsTest, VisitTasksConstTasks) {
  _task_a->set_as_predecessor_of(_task_b);
  const auto task_a = std::shared_ptr<const AbstractTask>(_task_a);
  const auto task_b = std::shared_ptr<const AbstractTask>(_task_b);

  auto expected_tasks = std::vector<std::shared_ptr<const AbstractTask>>{task_b, task_a};
  auto actual_tasks = std::vector<std::shared_ptr<const AbstractTask>>{};

  visit_tasks(task_b, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskVisitation::VisitPredecessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);

  actual_tasks.clear();
  std::reverse(expected_tasks.begin(), expected_tasks.end());

  visit_tasks_upwards(task_a, [&](const auto& task) {
    actual_tasks.emplace_back(task);
    return TaskUpwardVisitation::VisitSuccessors;
  });

  EXPECT_EQ(actual_tasks, expected_tasks);
}

}  // namespace hyrise
