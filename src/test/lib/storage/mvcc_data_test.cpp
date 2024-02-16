#include "base_test.hpp"

#include "storage/chunk.hpp"
#include "storage/mvcc_data.hpp"

namespace hyrise {

class MvccDataTest : public BaseTest {
 protected:
  void SetUp() override {
    _mvcc_data = std::make_shared<MvccData>(ChunkOffset{3}, CommitID{1});
    _mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{2});
    _mvcc_data->set_end_cid(ChunkOffset{0}, CommitID{2});

    _mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{3});
    _mvcc_data->set_end_cid(ChunkOffset{1}, CommitID{4});
    _mvcc_data->set_tid(ChunkOffset{1}, TransactionID{1});
  }

  std::shared_ptr<MvccData> _mvcc_data;
};

TEST_F(MvccDataTest, MemoryUsage) {
  // _mvcc_data stores at least:
  //   - 3 begin CIDs (uint32_t)
  //   - 3 end CIDs (uint32_t)
  //   - 3 TIDs (atomic uint32_t)
  //   - max begin and end CIDs (atomic uint32_t)
  //   - number of pending inserts (atomic uint32_t)
  // We do not check for the exact value to avoid calculating the memory overhead added by atomics, inlining, sizes of
  // the vector objects, and the actual vector capacity. Thus, the size should be larger than 12 * 4 B = 48 B.
  EXPECT_GT(_mvcc_data->memory_usage(), 48);
}

TEST_F(MvccDataTest, GetCIDsAndTIDs) {
  // Profound testing of MVCC functionality is part of the Insert/Delete tests. Concurrency tests can be found in
  // `stress_test.cpp`
  EXPECT_EQ(_mvcc_data->get_begin_cid(ChunkOffset{0}), CommitID{2});
  EXPECT_EQ(_mvcc_data->get_end_cid(ChunkOffset{0}), CommitID{2});
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{0}), INVALID_TRANSACTION_ID);

  EXPECT_EQ(_mvcc_data->get_begin_cid(ChunkOffset{1}), CommitID{3});
  EXPECT_EQ(_mvcc_data->get_end_cid(ChunkOffset{1}), CommitID{4});
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{1}), TransactionID{1});

  EXPECT_EQ(_mvcc_data->get_begin_cid(ChunkOffset{2}), CommitID{1});
  EXPECT_EQ(_mvcc_data->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{2}), INVALID_TRANSACTION_ID);

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(_mvcc_data->get_begin_cid(ChunkOffset{3}), std::logic_error);
    EXPECT_THROW(_mvcc_data->get_end_cid(ChunkOffset{3}), std::logic_error);
    EXPECT_THROW(_mvcc_data->get_tid(ChunkOffset{3}), std::logic_error);
  }
}

TEST_F(MvccDataTest, SetCIDsAndTIDs) {
  // Profound testing of MVCC functionality is part of the Insert/Delete tests. Concurrency tests can be found in
  // `stress_test.cpp`
  EXPECT_EQ(_mvcc_data->get_begin_cid(ChunkOffset{2}), CommitID{1});
  EXPECT_EQ(_mvcc_data->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{2}), INVALID_TRANSACTION_ID);

  _mvcc_data->set_begin_cid(ChunkOffset{2}, CommitID{5});
  _mvcc_data->set_end_cid(ChunkOffset{2}, CommitID{6});
  _mvcc_data->set_tid(ChunkOffset{2}, TransactionID{2});

  EXPECT_EQ(_mvcc_data->get_begin_cid(ChunkOffset{2}), CommitID{5});
  EXPECT_EQ(_mvcc_data->get_end_cid(ChunkOffset{2}), CommitID{6});
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{2}), TransactionID{2});

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(_mvcc_data->set_begin_cid(ChunkOffset{3}, CommitID{2}), std::logic_error);
    EXPECT_THROW(_mvcc_data->set_end_cid(ChunkOffset{3}, CommitID{3}), std::logic_error);
    EXPECT_THROW(_mvcc_data->set_tid(ChunkOffset{3}, TransactionID{2}), std::logic_error);
  }
}

TEST_F(MvccDataTest, CompareExchangeTID) {
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{0});

  // True: expected transaction ID is INVALID_TRANSACTION_ID.
  EXPECT_TRUE(_mvcc_data->compare_exchange_tid(ChunkOffset{0}, INVALID_TRANSACTION_ID, TransactionID{1}));
  EXPECT_EQ(_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{1});

  // False: expected transaction ID to be 0, but is 1.
  EXPECT_FALSE(_mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, TransactionID{2}));
  EXPECT_NE(_mvcc_data->get_tid(ChunkOffset{0}), TransactionID{2});

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(_mvcc_data->compare_exchange_tid(ChunkOffset{3}, TransactionID{0}, TransactionID{2}),
                 std::logic_error);
  }
}

TEST_F(MvccDataTest, Description) {
  _mvcc_data->set_end_cid(ChunkOffset{2}, CommitID{5});
  const auto expected_description = "TIDs: 0, 1, 0, \nBeginCIDs: 2, 3, 1, \nEndCIDs: 2, 4, 5, \n";
  auto stream = std::stringstream{};
  stream << *_mvcc_data;
  EXPECT_EQ(stream.str(), expected_description);
}

TEST_F(MvccDataTest, MaxBeginAndEndCID) {
  EXPECT_EQ(_mvcc_data->max_begin_cid.load(), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_mvcc_data->max_end_cid.load(), MvccData::MAX_COMMIT_ID);

  _mvcc_data->max_begin_cid = CommitID{1};
  _mvcc_data->max_end_cid = CommitID{2};

  EXPECT_EQ(_mvcc_data->max_begin_cid.load(), CommitID{1});
  EXPECT_EQ(_mvcc_data->max_end_cid.load(), CommitID{2});
}

TEST_F(MvccDataTest, PendingInserts) {
  EXPECT_EQ(_mvcc_data->pending_inserts(), 0);

  _mvcc_data->register_insert();
  EXPECT_EQ(_mvcc_data->pending_inserts(), 1);

  _mvcc_data->register_insert();
  EXPECT_EQ(_mvcc_data->pending_inserts(), 2);

  _mvcc_data->deregister_insert();
  EXPECT_EQ(_mvcc_data->pending_inserts(), 1);

  _mvcc_data->deregister_insert();
  EXPECT_EQ(_mvcc_data->pending_inserts(), 0);

  EXPECT_THROW(_mvcc_data->deregister_insert(), std::logic_error);
}

}  // namespace hyrise
