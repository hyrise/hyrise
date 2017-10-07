#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "types.hpp"

namespace opossum {

class TransactionContextTest : public BaseTest {
 protected:
  void SetUp() override {}

  void TearDown() override { TransactionManager::reset(); }

  TransactionManager& manager() { return TransactionManager::get(); }
};

TEST_F(TransactionContextTest, MarkAsFailedCanOnlyBeCalledOnActiveTransaction) {
  {
    auto context = manager().new_transaction_context();
    context->prepare_commit();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);

    context->commit();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);
  }

  {
    auto context = manager().new_transaction_context();
    context->mark_as_failed();
    context->mark_as_rolled_back();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);
  }

  {
    // Calling it twice is okay though.
    auto context = manager().new_transaction_context();
    context->mark_as_failed();
    context->mark_as_failed();
  }
}

TEST_F(TransactionContextTest, MarkAsFailedSucceedsOnlyWhenActive) {
  {
    auto context = manager().new_transaction_context();
    context->prepare_commit();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);

    context->commit();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);
  }

  {
    auto context = manager().new_transaction_context();
    context->mark_as_failed();
    context->mark_as_rolled_back();

    EXPECT_THROW(context->mark_as_failed(), std::logic_error);
  }

  {
    auto context = manager().new_transaction_context();

    // Calling it twice is okay though.
    EXPECT_TRUE(context->mark_as_failed());
    EXPECT_FALSE(context->mark_as_failed());
  }
}

TEST_F(TransactionContextTest, MarkAsRolledBackSucceedsOnlyWhenFailed) {
  {
    auto context = manager().new_transaction_context();

    EXPECT_THROW(context->mark_as_rolled_back(), std::logic_error);

    context->prepare_commit();

    EXPECT_THROW(context->mark_as_rolled_back(), std::logic_error);

    context->commit();

    EXPECT_THROW(context->mark_as_rolled_back(), std::logic_error);
  }

  {
    auto context = manager().new_transaction_context();
    context->mark_as_failed();

    // Calling it twice is okay though.
    EXPECT_TRUE(context->mark_as_rolled_back());
    EXPECT_FALSE(context->mark_as_rolled_back());
  }
}

TEST_F(TransactionContextTest, PrepareCommitOnlySucceedsIfActive) {
  {
    auto context = manager().new_transaction_context();

    // Calling it twice is okay.
    EXPECT_TRUE(context->prepare_commit());
    EXPECT_FALSE(context->prepare_commit());

    context->commit();
    EXPECT_THROW(context->prepare_commit(), std::logic_error);
  }

  {
    auto context = manager().new_transaction_context();

    context->mark_as_failed();
    EXPECT_THROW(context->prepare_commit(), std::logic_error);

    context->mark_as_rolled_back();
    EXPECT_THROW(context->prepare_commit(), std::logic_error);
  }
}

TEST_F(TransactionContextTest, CommitShouldCommitAllFollowingPendingTransactions) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  context_1->prepare_commit();
  context_2->prepare_commit();

  const auto prev_last_commit_id = manager().last_commit_id();

  context_2->commit();

  EXPECT_EQ(prev_last_commit_id, manager().last_commit_id());

  context_1->commit();

  EXPECT_EQ(context_2->commit_id(), manager().last_commit_id());
}

// Until the commit context is committed and the last commit id incremented,
// the commit context is held in a single linked list of shared_ptrs and hence not deleted.
TEST_F(TransactionContextTest, CommitContextGetsOnlyDeletedAfterCommitting) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  context_1->prepare_commit();
  context_2->prepare_commit();

  auto commit_context_1 = std::weak_ptr<CommitContext>(context_1->commit_context());
  auto commit_context_2 = std::weak_ptr<CommitContext>(context_2->commit_context());

  EXPECT_FALSE(commit_context_1.expired());
  EXPECT_FALSE(commit_context_2.expired());

  context_2->commit();
  context_2 = nullptr;

  EXPECT_FALSE(commit_context_1.expired());
  EXPECT_FALSE(commit_context_2.expired());

  context_1->commit();
  context_1 = nullptr;

  auto context_3 = manager().new_transaction_context();
  context_3->prepare_commit();

  EXPECT_TRUE(commit_context_1.expired());
  EXPECT_TRUE(commit_context_2.expired());
}

TEST_F(TransactionContextTest, CallbackFiresWhenCommitted) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  context_1->prepare_commit();
  context_2->prepare_commit();

  auto context_1_committed = false;
  auto callback_1 = [&context_1_committed](TransactionID) { context_1_committed = true; };

  auto context_2_committed = false;
  auto callback_2 = [&context_2_committed](TransactionID) { context_2_committed = true; };

  context_2->commit(callback_2);

  EXPECT_FALSE(context_2_committed);

  context_1->commit(callback_1);

  EXPECT_TRUE(context_1_committed);
  EXPECT_TRUE(context_2_committed);

  EXPECT_EQ(context_1->phase(), TransactionPhase::Committed);
  EXPECT_EQ(context_2->phase(), TransactionPhase::Committed);
}

}  // namespace opossum
