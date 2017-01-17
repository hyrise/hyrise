#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class ValidateTest : public BaseTest {
 protected:
  void SetUp() override {
    t = std::make_shared<Table>(Table(chunk_size));
    t->add_column("col_1", "int");
    t->add_column("col_1", "int");
    t->append({123, 456});

    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();

    validate = std::make_shared<Validate>(gt);
  }

  std::ostringstream output;

  std::string table_name = "validateTestTable";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable>(gt);
  std::shared_ptr<Table> t = nullptr;
  std::shared_ptr<Validate> validate;
};

// Legend:
// our_TID == row_TID, our_CID >= beg_CID, our_CID >= end_CID
// taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst

// yes, yes, yes
TEST_F(ValidateTest, Impossible) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 2;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 2;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 2;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// no, yes, yes
TEST_F(ValidateTest, PastDelete) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 42;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 2;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 2;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, no, yes
TEST_F(ValidateTest, Impossible2) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 2;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 4;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 1;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, yes, no
TEST_F(ValidateTest, OwnDeleteUncommitted) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 2;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 1;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 6;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// no, no, yes
TEST_F(ValidateTest, Impossible3) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 50;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 3;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 1;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, no, no
TEST_F(ValidateTest, OwnInsert) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 2;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 3;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 3;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 1u);
}

// no, yes, no
TEST_F(ValidateTest, PastInsertOrFutureDelete) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 99;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 2;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 3;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 1u);
}

// no, no, no
TEST_F(ValidateTest, UncommittedInsertOrFutureInsert) {
  auto context = TransactionContext(2, 2);

  t->get_chunk(0).mvcc_columns().tids[0] = 99;
  t->get_chunk(0).mvcc_columns().begin_cids[0] = 3;
  t->get_chunk(0).mvcc_columns().end_cids[0] = 3;

  validate->execute(&context);

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

}  // namespace opossum
