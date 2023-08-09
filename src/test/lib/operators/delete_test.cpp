#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OperatorsDeleteTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_name = "table_a";
    _table2_name = "table_b";
    _table3_name = "table_c";
    _table = load_table("resources/test_data/tbl/int_float.tbl");
    _table2 = load_table("resources/test_data/tbl/int_int3.tbl", ChunkOffset{3});
    _table3 = load_table("resources/test_data/tbl/25_ints_sorted.tbl", ChunkOffset{3});

    // Delete Operator works with the Storage Manager, so the test table must also be known to the StorageManager
    Hyrise::get().storage_manager.add_table(_table_name, _table);
    Hyrise::get().storage_manager.add_table(_table2_name, _table2);
    Hyrise::get().storage_manager.add_table(_table3_name, _table3);
  }

  std::string _table_name;
  std::string _table2_name;
  std::string _table3_name;
  std::shared_ptr<Table> _table;
  std::shared_ptr<Table> _table2;
  std::shared_ptr<Table> _table3;

  void helper(bool commit);
};

void OperatorsDeleteTest::helper(bool commit) {
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // Selects two out of three rows.
  auto gt = std::make_shared<GetTable>(_table_name);
  gt->execute();

  auto table_scan = create_table_scan(gt, ColumnID{1}, PredicateCondition::GreaterThan, 456.7f);
  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);

  delete_op->execute();

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{0}), transaction_context->transaction_id());
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{1}), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{2}), transaction_context->transaction_id());

  auto expected_end_cid = CommitID{0u};
  if (commit) {
    transaction_context->commit();
    expected_end_cid = transaction_context->commit_id();
  } else {
    transaction_context->rollback(RollbackReason::User);
    expected_end_cid = MvccData::MAX_COMMIT_ID;
  }

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{1}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{2}), expected_end_cid);

  auto expected_tid = commit ? transaction_context->transaction_id() : 0u;

  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{0}), expected_tid);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{1}), 0u);
  EXPECT_EQ(_table->get_chunk(ChunkID{0})->mvcc_data()->get_tid(ChunkOffset{2}), expected_tid);
}

TEST_F(OperatorsDeleteTest, ExecuteAndCommit) {
  helper(true);
}

TEST_F(OperatorsDeleteTest, ExecuteAndAbort) {
  helper(false);
}

TEST_F(OperatorsDeleteTest, DetectDirtyWrite) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt = std::make_shared<GetTable>(_table_name);
  gt->execute();

  auto table_scan1 = create_table_scan(gt, ColumnID{0}, PredicateCondition::Equals, "123");
  auto expected_result = create_table_scan(gt, ColumnID{0}, PredicateCondition::NotEquals, "123");
  auto table_scan2 = create_table_scan(gt, ColumnID{0}, PredicateCondition::LessThan, "1234");

  table_scan1->execute();
  expected_result->execute();
  table_scan2->execute();

  EXPECT_EQ(table_scan1->get_output()->chunk_count(), 1u);
  EXPECT_EQ(table_scan1->get_output()->get_chunk(ChunkID{0})->column_count(), 2u);

  auto delete_op1 = std::make_shared<Delete>(table_scan1);
  delete_op1->set_transaction_context(t1_context);

  auto delete_op2 = std::make_shared<Delete>(table_scan2);
  delete_op2->set_transaction_context(t2_context);

  delete_op1->execute();
  delete_op2->execute();

  EXPECT_FALSE(delete_op1->execute_failed());
  EXPECT_TRUE(delete_op2->execute_failed());

  // MVCC commit.
  t1_context->commit();
  t2_context->rollback(RollbackReason::Conflict);

  // Get validated table which should have only one row deleted.
  auto t_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt_post_delete = std::make_shared<GetTable>(_table_name);
  gt_post_delete->execute();

  auto validate = std::make_shared<Validate>(gt_post_delete);
  validate->set_transaction_context(t_context);

  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result->get_output());
}

TEST_F(OperatorsDeleteTest, EmptyDelete) {
  auto tx_context_modification = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt = std::make_shared<GetTable>(_table_name);
  gt->execute();

  auto table_scan = create_table_scan(gt, ColumnID{0}, PredicateCondition::Equals, "112233");

  table_scan->execute();

  EXPECT_EQ(table_scan->get_output()->chunk_count(), 0u);

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(tx_context_modification);

  delete_op->execute();

  EXPECT_FALSE(delete_op->execute_failed());

  // MVCC commit.
  tx_context_modification->commit();

  // Get validated table which should be the original one
  auto tx_context_verification = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt_post_delete = std::make_shared<GetTable>(_table_name);
  gt_post_delete->never_clear_output();
  gt_post_delete->execute();

  auto validate = std::make_shared<Validate>(gt_post_delete);
  validate->set_transaction_context(tx_context_verification);
  validate->never_clear_output();
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), gt_post_delete->get_output());
}

TEST_F(OperatorsDeleteTest, UpdateAfterDeleteFails) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt = std::make_shared<GetTable>(_table_name);
  gt->execute();

  auto validate1 = std::make_shared<Validate>(gt);
  validate1->set_transaction_context(t1_context);

  auto validate2 = std::make_shared<Validate>(gt);
  validate2->set_transaction_context(t2_context);

  validate1->execute();
  validate2->execute();

  auto delete_op = std::make_shared<Delete>(validate1);
  delete_op->set_transaction_context(t1_context);

  delete_op->execute();

  t1_context->commit();

  EXPECT_FALSE(delete_op->execute_failed());

  // this update tries to update the values that have been deleted in another transaction and should fail.
  auto update_op = std::make_shared<Update>(_table_name, validate2, validate2);
  update_op->set_transaction_context(t2_context);
  update_op->execute();
  EXPECT_TRUE(update_op->execute_failed());

  t2_context->rollback(RollbackReason::Conflict);
}

TEST_F(OperatorsDeleteTest, DeleteOwnInsert) {
  // We are testing a couple of things here:
  //   (1) When a transaction deletes a row that it inserted itself, it should no longer be visible to itself
  //   (2) When that transaction rolls back, the row should be invisible to a second transaction
  //   (3) The same should be the case if the transaction commits
  // For that purpose, we run the insert, delete, scan routine twice, once where we abort the transaction (inserted
  // and deleted value 456.7), and once where we commit it (inserted and deleted value 457.7)

  for (const auto value : {456.7f, 457.7f}) {
    auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    auto values_to_insert = load_table("resources/test_data/tbl/int_float3.tbl");
    auto table_name_for_insert = "bla";
    Hyrise::get().storage_manager.add_table(table_name_for_insert, values_to_insert);
    auto insert_get_table = std::make_shared<GetTable>(table_name_for_insert);
    insert_get_table->execute();

    auto insert = std::make_shared<Insert>(_table_name, insert_get_table);
    insert->set_transaction_context(context);
    insert->execute();

    auto gt = std::make_shared<GetTable>(_table_name);
    gt->execute();

    auto validate1 = std::make_shared<Validate>(gt);
    validate1->set_transaction_context(context);
    validate1->execute();

    auto table_scan1 = create_table_scan(validate1, ColumnID{1}, PredicateCondition::Equals, value);
    table_scan1->execute();
    EXPECT_EQ(table_scan1->get_output()->row_count(), 2);

    auto delete_op = std::make_shared<Delete>(table_scan1);
    delete_op->set_transaction_context(context);
    delete_op->execute();

    auto gt_post_delete = std::make_shared<GetTable>(_table_name);
    gt_post_delete->execute();

    auto validate2 = std::make_shared<Validate>(gt_post_delete);
    validate2->set_transaction_context(context);
    validate2->execute();

    auto table_scan2 = create_table_scan(validate2, ColumnID{1}, PredicateCondition::Equals, value);
    table_scan2->execute();
    EXPECT_EQ(table_scan2->get_output()->row_count(), 0);

    if (value == 456.7f) {
      context->rollback(RollbackReason::User);
    } else {
      context->commit();
    }

    Hyrise::get().storage_manager.drop_table(table_name_for_insert);
  }

  {
    auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    auto gt2 = std::make_shared<GetTable>(_table_name);
    gt2->execute();

    auto validate1 = std::make_shared<Validate>(gt2);
    validate1->set_transaction_context(context);
    validate1->execute();

    auto expected_result = load_table("resources/test_data/tbl/int_float_deleted.tbl");

    EXPECT_TABLE_EQ_UNORDERED(validate1->get_output(), expected_result);

    context->rollback(RollbackReason::User);
  }
}

// This test uses the transaction context after its already been committed on behalf of every
// read/write operator and the read only operators Validate and GetTable
TEST_F(OperatorsDeleteTest, UseTransactionContextAfterCommit) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  auto gt = std::make_shared<GetTable>(_table_name);
  auto validate = std::make_shared<Validate>(gt);
  auto delete_op = std::make_shared<Delete>(validate);
  auto delete_op2 = std::make_shared<Delete>(validate);
  delete_op->set_transaction_context_recursively(t1_context);

  gt->execute();
  validate->execute();
  delete_op->execute();

  t1_context->commit();

  delete_op2->set_transaction_context(t1_context);

  EXPECT_THROW(delete_op2->execute(), std::logic_error);
}

TEST_F(OperatorsDeleteTest, RunOnUnvalidatedTable) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();

  const auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::LessThan, 10'000);
  table_scan->never_clear_output();
  table_scan->execute();

  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto delete_op1 = std::make_shared<Delete>(table_scan);
  delete_op1->set_transaction_context(t1_context);
  // This one works and deletes some rows
  delete_op1->execute();
  t1_context->commit();

  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  auto delete_op2 = std::make_shared<Delete>(table_scan);
  delete_op2->set_transaction_context(t2_context);
  // This one should fail because the rows should have been filtered out by a validate and should not be visible
  // to the delete operator in the first place.
  EXPECT_THROW(delete_op2->execute(), std::logic_error);
  t2_context->rollback(RollbackReason::Conflict);
}

TEST_F(OperatorsDeleteTest, PrunedInputTable) {
  // Test that the input table of Delete can reference either a stored table or a pruned version of a stored table
  // (i.e., a table containing a subset of the chunks of the stored table)

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // Create the values_to_delete table via Chunk pruning and a Table Scan
  const auto get_table_op = std::make_shared<GetTable>("table_b", std::vector{ChunkID{1}}, std::vector<ColumnID>{});
  get_table_op->execute();

  const auto table_scan = create_table_scan(get_table_op, ColumnID{0}, PredicateCondition::LessThan, 5);
  table_scan->execute();

  const auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  transaction_context->commit();

  const auto expected_end_cid = transaction_context->commit_id();
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(_table2->get_chunk(ChunkID{0})->mvcc_data()->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->mvcc_data()->get_end_cid(ChunkOffset{0}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->mvcc_data()->get_end_cid(ChunkOffset{1}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{1})->mvcc_data()->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{2})->mvcc_data()->get_end_cid(ChunkOffset{0}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(_table2->get_chunk(ChunkID{2})->mvcc_data()->get_end_cid(ChunkOffset{1}), expected_end_cid);
}

TEST_F(OperatorsDeleteTest, SetMaxEndCID) {
  const auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();

  const auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::LessThan, 1000);
  table_scan->execute();

  const auto delete_op = std::make_shared<Delete>(table_scan);
  const auto transaction_context = std::make_shared<TransactionContext>(TransactionID{1}, CommitID{2}, AutoCommit::No);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();

  const auto& chunk = _table->get_chunk(ChunkID{0});
  ASSERT_TRUE(chunk->mvcc_data());
  EXPECT_EQ(chunk->mvcc_data()->max_end_cid.load(), MvccData::MAX_COMMIT_ID);

  transaction_context->commit();

  EXPECT_EQ(chunk->mvcc_data()->max_end_cid.load(), CommitID{2});
}

// Check that all MvccData and invalid_row_counts are correctly set for all kinds of PosLists.
TEST_F(OperatorsDeleteTest, DifferentPosLists) {
  // Case (i): RowIDPosList that references a single chunk.
  const auto pos_list_1 = std::make_shared<RowIDPosList>(
      RowIDPosList{RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}});
  pos_list_1->guarantee_single_chunk();
  const auto chunk_1 =
      std::make_shared<Chunk>(Segments{std::make_shared<ReferenceSegment>(_table3, ColumnID{0}, pos_list_1)});

  // Case (ii): RowIDPosList that references an entire chunk.
  const auto pos_list_2 = std::make_shared<RowIDPosList>(RowIDPosList{
      RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}}, RowID{ChunkID{1}, ChunkOffset{2}}});
  pos_list_2->guarantee_single_chunk();
  const auto chunk_2 =
      std::make_shared<Chunk>(Segments{std::make_shared<ReferenceSegment>(_table3, ColumnID{0}, pos_list_2)});

  // Case (iii): RowIDPosList that references multiple chunks.
  const auto pos_list_3 = std::make_shared<RowIDPosList>(RowIDPosList{
      RowID{ChunkID{2}, ChunkOffset{0}}, RowID{ChunkID{2}, ChunkOffset{1}}, RowID{ChunkID{3}, ChunkOffset{0}}});
  const auto chunk_3 =
      std::make_shared<Chunk>(Segments{std::make_shared<ReferenceSegment>(_table3, ColumnID{0}, pos_list_3)});

  // Case (iv): EntireChunkPosList that references an entire chunk.
  const auto pos_list_4 = std::make_shared<EntireChunkPosList>(ChunkID{4}, ChunkOffset{3});
  const auto chunk_4 =
      std::make_shared<Chunk>(Segments{std::make_shared<ReferenceSegment>(_table3, ColumnID{0}, pos_list_4)});

  // Case (v): EntireChunkPosList that does not reference an entire chunk.
  const auto pos_list_5 = std::make_shared<EntireChunkPosList>(ChunkID{5}, ChunkOffset{2});
  const auto chunk_5 =
      std::make_shared<Chunk>(Segments{std::make_shared<ReferenceSegment>(_table3, ColumnID{0}, pos_list_5)});

  // Build a reference table as input for the operator.
  auto chunks = std::vector<std::shared_ptr<Chunk>>{chunk_1, chunk_2, chunk_3, chunk_4, chunk_5};
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
  const auto reference_table = std::make_shared<Table>(column_definitions, TableType::References, std::move(chunks));
  const auto table_wrapper = std::make_shared<TableWrapper>(reference_table);

  // Execute the operator.
  const auto delete_op = std::make_shared<Delete>(table_wrapper);
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  delete_op->set_transaction_context(transaction_context);
  execute_all({table_wrapper, delete_op});
  transaction_context->commit();
  const auto expected_end_cid = transaction_context->commit_id();

  const auto& mvcc_data_1 = _table3->get_chunk(ChunkID{0})->mvcc_data();
  EXPECT_EQ(mvcc_data_1->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_1->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(mvcc_data_1->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(mvcc_data_1->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{0})->invalid_row_count(), 2);

  const auto& mvcc_data_2 = _table3->get_chunk(ChunkID{1})->mvcc_data();
  EXPECT_EQ(mvcc_data_2->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_2->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(mvcc_data_2->get_end_cid(ChunkOffset{2}), expected_end_cid);
  EXPECT_EQ(mvcc_data_2->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{1})->invalid_row_count(), 3);

  const auto& mvcc_data_3 = _table3->get_chunk(ChunkID{2})->mvcc_data();
  EXPECT_EQ(mvcc_data_3->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_3->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(mvcc_data_3->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(mvcc_data_3->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{2})->invalid_row_count(), 2);

  const auto& mvcc_data_4 = _table3->get_chunk(ChunkID{3})->mvcc_data();
  EXPECT_EQ(mvcc_data_4->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_4->get_end_cid(ChunkOffset{1}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(mvcc_data_4->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(mvcc_data_4->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{3})->invalid_row_count(), 1);

  const auto& mvcc_data_5 = _table3->get_chunk(ChunkID{4})->mvcc_data();
  EXPECT_EQ(mvcc_data_5->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_5->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(mvcc_data_5->get_end_cid(ChunkOffset{2}), expected_end_cid);
  EXPECT_EQ(mvcc_data_5->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{4})->invalid_row_count(), 3);

  const auto& mvcc_data_6 = _table3->get_chunk(ChunkID{5})->mvcc_data();
  EXPECT_EQ(mvcc_data_6->get_end_cid(ChunkOffset{0}), expected_end_cid);
  EXPECT_EQ(mvcc_data_6->get_end_cid(ChunkOffset{1}), expected_end_cid);
  EXPECT_EQ(mvcc_data_6->get_end_cid(ChunkOffset{2}), MvccData::MAX_COMMIT_ID);
  EXPECT_EQ(mvcc_data_6->max_end_cid.load(), expected_end_cid);
  EXPECT_EQ(_table3->get_chunk(ChunkID{5})->invalid_row_count(), 2);

  const auto chunk_count = _table3->chunk_count();
  for (auto chunk_id = ChunkID{6}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = _table3->get_chunk(chunk_id);
    const auto& mvcc_data = _table3->get_chunk(chunk_id)->mvcc_data();
    const auto chunk_size = chunk->size();

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      EXPECT_EQ(mvcc_data->get_end_cid(chunk_offset), MvccData::MAX_COMMIT_ID);
    }
    EXPECT_EQ(mvcc_data->max_end_cid.load(), MvccData::MAX_COMMIT_ID);
    EXPECT_EQ(_table3->get_chunk(chunk_id)->invalid_row_count(), 0);
  }
}

// We want to notice if the input references rows multiple times. Though this is no problem per se, it currently leads
// to an incorrect invalid_row_count of the affected chunk.
TEST_F(OperatorsDeleteTest, DuplicateRows) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();
  // Create TableScan that selects only the second row.
  const auto column_a = get_column_expression(get_table, ColumnID{0});
  const auto table_scan = std::make_shared<TableScan>(get_table, equals_(column_a, 123));
  table_scan->never_clear_output();
  // Create UnionAll to have the row twice in the resulting table.
  const auto union_all = std::make_shared<UnionAll>(table_scan, table_scan);
  union_all->never_clear_output();

  const auto delete_op = std::make_shared<Delete>(union_all);
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  delete_op->set_transaction_context(transaction_context);

  execute_all({table_scan, union_all});
  EXPECT_EQ(table_scan->get_output()->row_count(), 1);
  EXPECT_EQ(union_all->get_output()->row_count(), 2);

  EXPECT_THROW(delete_op->execute(), std::logic_error);
  transaction_context->rollback(RollbackReason::Conflict);
}

}  // namespace hyrise
