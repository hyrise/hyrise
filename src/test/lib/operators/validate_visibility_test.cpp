#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"

namespace hyrise {

class OperatorsValidateVisibilityTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}};
    table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
    table->append({123, 456});
    const auto table_id = Hyrise::get().catalog.add_table("validateTestTable", table);
    const auto get_table = std::make_shared<GetTable>(table_id);
    get_table->execute();

    validate = std::make_shared<Validate>(get_table);
  }

  static constexpr auto chunk_size = ChunkOffset{10};

  std::shared_ptr<Table> table;
  std::shared_ptr<Validate> validate;
};

// Legend:
// our_TID == row_TID, our_CID >= beg_CID, our_CID >= end_CID
// taken from: https://github.com/hyrise/hyrise-v1/blob/master/docs/documentation/queryexecution/tx.rst

// yes, yes, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{2});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

// no, yes, yes
TEST_F(OperatorsValidateVisibilityTest, PastDelete) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{42});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{2});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

// yes, no, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible2) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{4});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{1});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

// yes, yes, no
TEST_F(OperatorsValidateVisibilityTest, OwnDeleteUncommitted) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{1});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{6});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

// no, no, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible3) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{50});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{3});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{1});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

// yes, no, no
TEST_F(OperatorsValidateVisibilityTest, OwnInsert) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{3});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{3});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 1);
}

// no, yes, no
TEST_F(OperatorsValidateVisibilityTest, PastInsertOrFutureDelete) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{99});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{2});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{3});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 1);
}

// no, no, no
TEST_F(OperatorsValidateVisibilityTest, UncommittedInsertOrFutureInsert) {
  auto context = std::make_shared<TransactionContext>(TransactionID{2}, CommitID{2}, AutoCommit::No);

  table->get_chunk(ChunkID{0})->mvcc_data()->set_tid(ChunkOffset{0}, TransactionID{99});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_begin_cid(ChunkOffset{0}, CommitID{3});
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{3});

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0);
}

}  // namespace hyrise
