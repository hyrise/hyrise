#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "../base_test.hpp"
#include "tuning/abstract_evaluator.hpp"
#include "tuning/abstract_selector.hpp"
#include "tuning/tuner.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

namespace {

using Runtime = std::chrono::milliseconds;
using RuntimeMs = std::chrono::milliseconds::rep;
using RuntimeMsList = std::vector<std::chrono::milliseconds::rep>;

struct MockOperation : public TuningOperation {
  explicit MockOperation(RuntimeMs runtime) : runtime{runtime} {}

  void execute() final { std::this_thread::sleep_for(runtime); }

  Runtime runtime;
};

struct MockSelector : public AbstractSelector {
  explicit MockSelector(RuntimeMs runtime, RuntimeMsList operation_runtimes) : runtime{runtime} {
    operations.clear();
    operations.reserve(operation_runtimes.size());
    for (auto operation_runtime : operation_runtimes) {
      operations.push_back(std::make_shared<MockOperation>(operation_runtime));
    }
  }

  std::vector<std::shared_ptr<TuningOperation>> select(const std::vector<std::shared_ptr<TuningChoice>>& choices,
                                                       float budget) final {
    std::this_thread::sleep_for(runtime);

    return operations;
  }

  Runtime runtime;
  std::vector<std::shared_ptr<TuningOperation>> operations;
};

struct MockEvaluator : public AbstractEvaluator {
  explicit MockEvaluator(RuntimeMs runtime) : runtime{runtime} {}

  void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) final { std::this_thread::sleep_for(runtime); }

  Runtime runtime;
};

}  // namespace

class TunerTest : public BaseTest {};

TEST_F(TunerTest, TuningCompleted) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockSelector>(1, RuntimeMsList{1, 1, 1}));

  tuner.set_time_budget(0.1);  // in seconds

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::Completed);
}

TEST_F(TunerTest, TuningOverallTimeout) {
  // during evaluation phase
  Tuner tuner_eval;
  tuner_eval.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_eval.add_evaluator(std::make_unique<MockEvaluator>(200));
  tuner_eval.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_eval.set_selector(std::make_unique<MockSelector>(1, RuntimeMsList{1, 1, 1}));

  tuner_eval.set_time_budget(0.1);  // in seconds

  tuner_eval.schedule_tuning_process();
  tuner_eval.wait_for_completion();

  EXPECT_EQ(tuner_eval.status(), Tuner::Status::Timeout);

  // during selection phase
  Tuner tuner_sel;
  tuner_sel.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_sel.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_sel.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_sel.set_selector(std::make_unique<MockSelector>(200, RuntimeMsList{1, 1, 1}));

  tuner_sel.set_time_budget(0.1);  // in seconds

  tuner_sel.schedule_tuning_process();
  tuner_sel.wait_for_completion();

  EXPECT_EQ(tuner_sel.status(), Tuner::Status::Timeout);

  // during execution phase
  Tuner tuner_exec;
  tuner_exec.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_exec.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_exec.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner_exec.set_selector(std::make_unique<MockSelector>(1, RuntimeMsList{1, 200, 1}));

  tuner_exec.set_time_budget(0.1);  // in seconds

  tuner_exec.schedule_tuning_process();
  tuner_exec.wait_for_completion();

  EXPECT_EQ(tuner_exec.status(), Tuner::Status::Timeout);
}

TEST_F(TunerTest, TuningEvaluationTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(200));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockSelector>(200, RuntimeMsList{1, 1, 1}));

  tuner.set_time_budget(0.3, Tuner::NoBudget, 0.1, Tuner::NoBudget);  // in seconds

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::EvaluationTimeout);
}

TEST_F(TunerTest, TuningSelectionTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockSelector>(200, RuntimeMsList{1, 200, 1}));

  tuner.set_time_budget(0.3, Tuner::NoBudget, Tuner::NoBudget, 0.1);  // in seconds

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::SelectionTimeout);
}

TEST_F(TunerTest, TuningExecutionTimeout) {
  Tuner tuner;
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.add_evaluator(std::make_unique<MockEvaluator>(1));
  tuner.set_selector(std::make_unique<MockSelector>(1, RuntimeMsList{200, 200, 200}));

  tuner.set_time_budget(0.3, 0.1);  // in seconds

  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  EXPECT_EQ(tuner.status(), Tuner::Status::ExecutionTimeout);
}

}  // namespace opossum
