#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/validate.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

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
  }

  std::ostringstream output;

  std::string table_name = "validateTestTable";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable>(gt);
  std::shared_ptr<Table> t = nullptr;
};

// Legend:
// our_TID == our_TID, our_CID >= beg_CID, our_CID >= end_CID
// taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst

// yes, yes, yes
TEST_F(ValidateTest, Impossible) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 2;
  t->get_chunk(0)._begin_CIDs[0] = 2;
  t->get_chunk(0)._end_CIDs[0] = 2;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // Check that is_visible works
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

// no, yes, yes
TEST_F(ValidateTest, PastDelete) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 42;
  t->get_chunk(0)._begin_CIDs[0] = 2;
  t->get_chunk(0)._end_CIDs[0] = 2;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // Check that is_visible works
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

// yes, no, yes
TEST_F(ValidateTest, Impossible2) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 2;
  t->get_chunk(0)._begin_CIDs[0] = 4;
  t->get_chunk(0)._end_CIDs[0] = 1;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // Check that is_visible works
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

// yes, yes, no
TEST_F(ValidateTest, OwnDeleteUncommitted) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 2;
  t->get_chunk(0)._begin_CIDs[0] = 1;
  t->get_chunk(0)._end_CIDs[0] = 6;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // Check that is_visible works
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

// no, no, yes
TEST_F(ValidateTest, Impossible3) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 50;
  t->get_chunk(0)._begin_CIDs[0] = 3;
  t->get_chunk(0)._end_CIDs[0] = 1;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // Check that is_visible works
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

// yes, no, no
TEST_F(ValidateTest, OwnInsert) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 2;
  t->get_chunk(0)._begin_CIDs[0] = 3;
  t->get_chunk(0)._end_CIDs[0] = 3;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // check that row has been unserted
  EXPECT_EQ(vali->get_output()->row_count(), 1u);
}

// no, yes, no
TEST_F(ValidateTest, PastInsertOrFutureDelete) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 99;
  t->get_chunk(0)._begin_CIDs[0] = 2;
  t->get_chunk(0)._end_CIDs[0] = 3;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // check that row has been unserted
  EXPECT_EQ(vali->get_output()->row_count(), 1u);
}

// no, no, no
TEST_F(ValidateTest, UncommittedInsertOrFutureInsert) {
  auto vali = std::make_shared<Validate>(gt);
  auto context = TransactionContext(2, 2);

  t->get_chunk(0)._TIDs[0] = 99;
  t->get_chunk(0)._begin_CIDs[0] = 3;
  t->get_chunk(0)._end_CIDs[0] = 3;

  EXPECT_EQ(t->row_count(), 1u);

  vali->execute(&context);

  // check that row has been unserted
  EXPECT_EQ(vali->get_output()->row_count(), 0u);
}

}  // namespace opossum
