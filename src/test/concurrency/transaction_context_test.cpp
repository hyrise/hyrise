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
};

TEST_F(TransactionContextTest, CidThrowsExceptionBeforePrepareCommit) {
  auto context = TransactionManager::get().new_transaction_context();

  EXPECT_EQ(context->phase(), TransactionPhase::Active);
  EXPECT_ANY_THROW(context->cid());
}

TEST_F(TransactionContextTest, CidDoesNotThrowAfterPrepareCommit) {
  auto context = TransactionManager::get().new_transaction_context();

  context->prepare_commit();

  EXPECT_EQ(context->phase(), TransactionPhase::Committing);
  EXPECT_NO_THROW(context->cid());
}

TEST_F(TransactionContextTest, CannotAbortWhenCommitting) {
  auto context = TransactionManager::get().new_transaction_context();

  context->prepare_commit();

  EXPECT_EQ(context->phase(), TransactionPhase::Committing);
  EXPECT_ANY_THROW(context->abort());
}

TEST_F(TransactionContextTest, CommitCommitsAllPendingTransaction) {
  auto& manager = TransactionManager::get();
  auto context_1 = manager.new_transaction_context();
  auto context_2 = manager.new_transaction_context();

  context_1->prepare_commit();
  context_2->prepare_commit();

  const auto prev_lcid = manager.lcid();

  context_2->commit();

  EXPECT_EQ(prev_lcid, manager.lcid());

  context_1->commit();

  EXPECT_EQ(context_2->cid(), manager.lcid());
}

}  // namespace opossum
