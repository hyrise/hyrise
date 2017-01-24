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

class TransactionManagerTest : public BaseTest {
 protected:
  void SetUp() override {}

  void TearDown() override { TransactionManager::reset(); }

  TransactionManager& manager() { return TransactionManager::get(); }
};

TEST_F(TransactionManagerTest, NonActiveTransactionCannotBeAborted) {
  auto context = manager().new_transaction_context();

  manager().prepare_commit(*context);

  EXPECT_EQ(context->phase(), TransactionPhase::Committing);
  EXPECT_ANY_THROW(manager().abort(*context));
}

TEST_F(TransactionManagerTest, CommitShouldCommitAllFollowingPendingTransactions) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  manager().prepare_commit(*context_1);
  manager().prepare_commit(*context_2);

  const auto prev_last_commit_id = manager().last_commit_id();

  manager().commit(*context_2);

  EXPECT_EQ(prev_last_commit_id, manager().last_commit_id());

  manager().commit(*context_1);

  EXPECT_EQ(context_2->commit_id(), manager().last_commit_id());
}

// Until the commit context is committed and the last commit id incremented,
// the commit context is held in a single linked list of shared_ptrs and hence not deleted.
TEST_F(TransactionManagerTest, CommitContextGetsOnlyDeletedAfterCommitting) {
  auto context_1 = manager().new_transaction_context();
  auto context_2 = manager().new_transaction_context();

  manager().prepare_commit(*context_1);
  manager().prepare_commit(*context_2);

  auto commit_context_1 = std::weak_ptr<CommitContext>(context_1->commit_context());
  auto commit_context_2 = std::weak_ptr<CommitContext>(context_2->commit_context());

  EXPECT_FALSE(commit_context_1.expired());
  EXPECT_FALSE(commit_context_2.expired());

  manager().commit(*context_2);
  context_2 = nullptr;

  EXPECT_FALSE(commit_context_1.expired());
  EXPECT_FALSE(commit_context_2.expired());

  manager().commit(*context_1);
  context_1 = nullptr;

  auto context_3 = manager().new_transaction_context();
  manager().prepare_commit(*context_3);

  EXPECT_TRUE(commit_context_1.expired());
  EXPECT_TRUE(commit_context_2.expired());
}

}  // namespace opossum
