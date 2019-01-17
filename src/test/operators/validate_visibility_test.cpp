#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "concurrency/transaction_context.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsValidateVisibilityTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Int);
    t = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
    t->append({123, 456});

    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();

    validate = std::make_shared<Validate>(gt);
  }

  std::string table_name = "validateTestTable";

  static constexpr auto chunk_size = uint32_t{10};

  std::shared_ptr<GetTable> gt;
  std::shared_ptr<Table> t;
  std::shared_ptr<Validate> validate;
};

// Legend:
// our_TID == row_TID, our_CID >= beg_CID, our_CID >= end_CID
// taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst

// yes, yes, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 2;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// no, yes, yes
TEST_F(OperatorsValidateVisibilityTest, PastDelete) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 42;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 2;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, no, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible2) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 4;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 1;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, yes, no
TEST_F(OperatorsValidateVisibilityTest, OwnDeleteUncommitted) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 1;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 6;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// no, no, yes
TEST_F(OperatorsValidateVisibilityTest, Impossible3) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 50;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 3;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 1;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

// yes, no, no
TEST_F(OperatorsValidateVisibilityTest, OwnInsert) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 3;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 3;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 1u);
}

// no, yes, no
TEST_F(OperatorsValidateVisibilityTest, PastInsertOrFutureDelete) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 99;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 2;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 3;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 1u);
}

// no, no, no
TEST_F(OperatorsValidateVisibilityTest, UncommittedInsertOrFutureInsert) {
  auto context = std::make_shared<TransactionContext>(2, 2);

  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->tids[0] = 99;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->begin_cids[0] = 3;
  t->get_chunk(ChunkID{0})->get_scoped_mvcc_data_lock()->end_cids[0] = 3;

  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_EQ(validate->get_output()->row_count(), 0u);
}

}  // namespace opossum
