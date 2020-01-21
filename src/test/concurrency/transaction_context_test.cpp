#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContextTest : public BaseTest {
 public:
  static constexpr auto table_name = "test_table";

 protected:
  void SetUp() override {
    auto t = load_table("resources/test_data/tbl/float_int.tbl");
    // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
    Hyrise::get().storage_manager.add_table(table_name, t);
  }

  TransactionManager& manager() { return Hyrise::get().transaction_manager; }
};

/**
 * @brief Helper operator.
 *
 * It calls the functor between a context’s assignment of a commit ID and it final commit.
 */
class CommitFuncOp : public AbstractReadWriteOperator {
 public:
  explicit CommitFuncOp(std::function<void()> func) : AbstractReadWriteOperator(OperatorType::Mock), _func{func} {}

  const std::string& name() const override {
    static const auto name = std::string{"CommitOp"};
    return name;
  }

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override { return nullptr; }

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

TEST_F(TransactionContextTest, CommitShouldIncreaseCommitIDIfReadWrite) {
  auto context = manager().new_transaction_context();

  const auto prev_last_commit_id = manager().last_commit_id();

  const auto get_table_op = std::make_shared<GetTable>(table_name);
  const auto validate_op = std::make_shared<Validate>(get_table_op);
  const auto delete_op = std::make_shared<Delete>(validate_op);
  delete_op->set_transaction_context_recursively(context);
  get_table_op->execute();
  validate_op->execute();
  delete_op->execute();

  context->commit();

  EXPECT_EQ(manager().last_commit_id(), prev_last_commit_id + 1);
}

TEST_F(TransactionContextTest, CommitShouldNotIncreaseCommitIDIfReadOnly) {
  auto context = manager().new_transaction_context();

  const auto prev_last_commit_id = manager().last_commit_id();

  const auto get_table_op = std::make_shared<GetTable>(table_name);
  const auto validate_op = std::make_shared<Validate>(get_table_op);
  validate_op->set_transaction_context_recursively(context);
  get_table_op->execute();
  validate_op->execute();

  context->commit();

  EXPECT_EQ(manager().last_commit_id(), prev_last_commit_id);
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

TEST_F(TransactionContextTest, CommitWithFailedOperator) {
  auto context = manager().new_transaction_context();
  context->rollback();
  EXPECT_ANY_THROW(context->commit());
}

}  // namespace opossum
