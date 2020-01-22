#include <algorithm>
#include <vector>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"

namespace opossum {

class TransactionManagerTest : public BaseTest {
 protected:
  void SetUp() override {}

  static std::unordered_multiset<CommitID>& get_active_snapshot_commit_ids() {
    return Hyrise::get().transaction_manager._active_snapshot_commit_ids;
  }

  static void register_transaction(CommitID snapshot_commit_id) {
    Hyrise::get().transaction_manager._register_transaction(snapshot_commit_id);
  }
  static void deregister_transaction(CommitID snapshot_commit_id) {
    Hyrise::get().transaction_manager._deregister_transaction(snapshot_commit_id);
  }
};

/** Check if all active snapshot commit ids of uncommitted
 * transaction contexts are tracked correctly.
 * Normally, deregister_transaction() is called in the
 * destructor of the transaction context but is called
 * manually for this test.
 */
TEST_F(TransactionManagerTest, TrackActiveCommitIDs) {
  auto& manager = Hyrise::get().transaction_manager;

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 0);
  EXPECT_EQ(manager.get_lowest_active_snapshot_commit_id(), std::nullopt);

  const auto t1_context = manager.new_transaction_context();
  const auto t2_context = manager.new_transaction_context();
  const auto t3_context = manager.new_transaction_context();

  const CommitID t1_snapshot_commit_id = t1_context->snapshot_commit_id();
  const CommitID t2_snapshot_commit_id = t2_context->snapshot_commit_id();
  const CommitID t3_snapshot_commit_id = t3_context->snapshot_commit_id();
  const auto vec = std::vector<CommitID>{t1_snapshot_commit_id, t2_snapshot_commit_id, t3_snapshot_commit_id};

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 3);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t1_snapshot_commit_id) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t2_snapshot_commit_id) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t3_snapshot_commit_id) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager.get_lowest_active_snapshot_commit_id(), *std::min_element(vec.cbegin(), vec.cend()));

  t1_context->commit();
  deregister_transaction(t1_context->snapshot_commit_id());

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 2);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t1_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t3_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager.get_lowest_active_snapshot_commit_id(), t2_context->snapshot_commit_id());

  t3_context->commit();
  deregister_transaction(t3_context->snapshot_commit_id());

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 1);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(),
                        t2_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager.get_lowest_active_snapshot_commit_id(), t2_context->snapshot_commit_id());

  t2_context->commit();
  deregister_transaction(t2_context->snapshot_commit_id());

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 0);
  EXPECT_EQ(manager.get_lowest_active_snapshot_commit_id(), std::nullopt);

  // To prevent exceptions in TransactionContext destructor
  register_transaction(t1_snapshot_commit_id);
  register_transaction(t2_snapshot_commit_id);
  register_transaction(t3_snapshot_commit_id);
}

}  // namespace opossum
