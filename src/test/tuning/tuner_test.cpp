#include "../base_test.hpp"

#include <chrono>
#include <thread>

#include "tuning/abstract_evaluator.hpp"
#include "tuning/abstract_selector.hpp"
#include "tuning/tuner.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

class MockOperation : public TuningOperation {
  MockEvaluator(std::chrono::milliseconds::rep runtime) : runtime{runtime} {}

  void execute() final { std::this_thread::sleep_for(runtime); }

  std::chrono::milliseconds runtime;
};

class MockSelector : public AbstractSelector {
  MockSelector(std::chrono::milliseconds::rep runtime,
               std::initializer_list<std::chrono::milliseconds::rep> operation_runtimes)
      : runtime{runtime} {
    operations.clear();
    operations.reserve(operation_runtimes.size());
    for (auto operation_runtime : operation_runtimes) {
      operations.emplace_back(operation_runtime);
    }
  }

  std::vector<std::shared_ptr<TuningOperation>> select(const std::vector<std::shared_ptr<TuningChoice>>& choices,
                                                       float budget) final {
    std::this_thread::sleep_for(runtime);

    return operations;
  }

  std::chrono::milliseconds runtime;
  std::vector<MockOperation> operations;
};

class MockEvaluator : public AbstractEvaluator {
  MockEvaluator(std::chrono::milliseconds::rep runtime) : runtime{runtime} {}

  void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) final { std::this_thread::sleep_for(runtime); }

  std::chrono::milliseconds runtime;
};

class TunerTest : public BaseTest {};

TEST_F(TunerTest, TuningCompleted) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(1, {1, 1, 1}));

  tuner.set_time_budget(100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::Completed);
}

TEST_F(TunerTest, TuningOverallTimeout) {
  Tuner tuner;
  // during evaluation phase
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(200));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(1, {1, 1, 1}));

  tuner.set_time_budget(100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::Timeout);

  // during selection phase
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(200, {1, 1, 1}));

  tuner.set_time_budget(100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::Timeout);

  // during execution phase
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(1, {1, 200, 1}));

  tuner.set_time_budget(100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::Timeout);
}

TEST_F(TunerTest, TuningEvaluationTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(200));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(200, {1, 1, 1}));

  tuner.set_time_budget(300, Tuner::NoBudget, 100, Tuner::NoBudget);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::EvaluationTimeout);
}

TEST_F(TunerTest, TuningSelectionTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(200, {1, 200, 1}));

  tuner.set_time_budget(300, Tuner::NoBudget, Tuner::NoBudget, 100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::SelectionTimeout);
}

TEST_F(TunerTest, TuningExecutionTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockEvaluator>(1, {200, 200, 200}));

  tuner.set_time_budget(300, 100);

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::ExecutionTimeout);
}

}  // namespace opossum
