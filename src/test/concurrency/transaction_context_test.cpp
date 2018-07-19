#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContextTest : public BaseTest {
 protected:
  void SetUp() override {}

  TransactionManager& manager() { return TransactionManager::get(); }
};

/**
 * @brief Helper operator.
 *
 * It calls the functor between a context’s assignment of a commit ID and it final commit.
 */
class CommitFuncOp : public AbstractReadWriteOperator {
 public:
  explicit CommitFuncOp(std::function<void()> func) : AbstractReadWriteOperator(OperatorType::Mock), _func{func} {}

  const std::string name() const override { return "CommitOp"; }

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override {
    context->register_read_write_operator(std::static_pointer_cast<AbstractReadWriteOperator>(shared_from_this()));
    return nullptr;
  }

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override {
    Fail("Unexpected function call");
  }

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  void _on_commit_records(const CommitID cid) override { _func(); }

  void _on_rollback_records() override {}

 private:
  std::function<void()> _func;
};

TEST_F(TransactionContextTest, CommitShouldCommitAllFollowingPendingTransactions) {
  const auto empty_callback = [](TransactionID) {};

  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  const auto prev_last_commit_id = manager().last_commit_id();

  auto try_commit_context_2 = [&]() {
    context_2->commit_async(empty_callback);

    EXPECT_EQ(prev_last_commit_id, manager().last_commit_id());
  };

  auto commit_op = std::make_shared<CommitFuncOp>(try_commit_context_2);
  commit_op->set_transaction_context(context_1);
  commit_op->execute();

  /**
   * Execution order
   *
   * - context_1 gets commit ID
   * - context_2 gets commit ID
   * - context_2 tries to commit, but needs to wait for context_1
   * - context_1 commits, followed by context_2
   *
   */
  context_1->commit_async(empty_callback);

  EXPECT_EQ(context_2->commit_id(), manager().last_commit_id());
}

TEST_F(TransactionContextTest, CallbackFiresWhenCommitted) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  auto context_1_committed = false;
  auto callback_1 = [&context_1_committed](TransactionID) { context_1_committed = true; };

  auto context_2_committed = false;
  auto callback_2 = [&context_2_committed](TransactionID) { context_2_committed = true; };

  context_2->commit_async(callback_2);
  context_1->commit_async(callback_1);

  EXPECT_TRUE(context_1_committed);
  EXPECT_TRUE(context_2_committed);

  EXPECT_EQ(context_1->phase(), TransactionPhase::Committed);
  EXPECT_EQ(context_2->phase(), TransactionPhase::Committed);
}

}  // namespace opossum
