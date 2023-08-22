#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OperatorsInsertTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsInsertTest, SelfInsert) {
  auto table_name = "test_table";
  auto table = load_table("resources/test_data/tbl/float_int.tbl");
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto get_table = std::make_shared<GetTable>(table_name);
  get_table->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);

  insert->execute();

  context->commit();

  // Check that row has been inserted.
  EXPECT_EQ(table->row_count(), 6u);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->size(), 3u);
  EXPECT_EQ((*table->get_chunk(ChunkID{0})->get_segment(ColumnID{1}))[ChunkOffset{0}], AllTypeVariant(12345));
  EXPECT_EQ((*table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}))[ChunkOffset{0}], AllTypeVariant(458.7f));
  EXPECT_EQ((*table->get_chunk(ChunkID{1})->get_segment(ColumnID{1}))[ChunkOffset{0}], AllTypeVariant(12345));
  EXPECT_EQ((*table->get_chunk(ChunkID{1})->get_segment(ColumnID{0}))[ChunkOffset{0}], AllTypeVariant(458.7f));

  EXPECT_EQ(table->get_chunk(ChunkID{0})->get_segment(ColumnID{0})->size(), 3u);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->get_segment(ColumnID{1})->size(), 3u);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->get_segment(ColumnID{0})->size(), 3u);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->get_segment(ColumnID{1})->size(), 3u);
}

TEST_F(OperatorsInsertTest, InsertRespectChunkSize) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  // 3 Rows, chunk_size = 4
  auto table = load_table("resources/test_data/tbl/int.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name, table);

  // 10 Rows
  auto table2 = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 4u);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ(table->get_chunk(ChunkID{3})->size(), 2u);
  EXPECT_EQ(table->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, MultipleChunks) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  // 3 Rows
  auto table = load_table("resources/test_data/tbl/int.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table(table_name, table);

  // 10 Rows
  auto table2 = load_table("resources/test_data/tbl/10_ints.tbl", ChunkOffset{3});
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 7u);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(table->get_chunk(ChunkID{6})->size(), 2u);
  EXPECT_EQ(table->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, CompressedChunks) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  // 3 Rows
  auto table = load_table("resources/test_data/tbl/int.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table(table_name, table);
  ChunkEncoder::encode_all_chunks(table);

  // 10 Rows
  auto table2 = load_table("resources/test_data/tbl/10_ints.tbl");
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 7u);
  EXPECT_EQ(table->get_chunk(ChunkID{6})->size(), 2u);
  EXPECT_EQ(table->row_count(), 13u);
}

TEST_F(OperatorsInsertTest, Rollback) {
  auto table_name = "test3";

  auto table = load_table("resources/test_data/tbl/int.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto get_table1 = std::make_shared<GetTable>(table_name);
  get_table1->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table1);
  auto context1 = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context1);
  insert->execute();

  const auto check = [&]() {
    auto get_table2 = std::make_shared<GetTable>(table_name);
    get_table2->execute();
    auto validate = std::make_shared<Validate>(get_table2);
    auto context2 = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    validate->set_transaction_context(context2);
    validate->execute();
    EXPECT_EQ(validate->get_output()->row_count(), 3u);
  };
  check();

  context1->rollback(RollbackReason::User);
  check();
}

TEST_F(OperatorsInsertTest, RollbackIncreaseInvalidRowCount) {
  auto t_name = "test1";

  // Set Up
  auto t = load_table("resources/test_data/tbl/int.tbl", ChunkOffset{10});
  Hyrise::get().storage_manager.add_table(t_name, t);
  auto row_count = t->row_count();
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->chunk_count(), 1);

  // Insert rows again
  auto gt1 = std::make_shared<GetTable>(t_name);
  gt1->execute();
  auto ins = std::make_shared<Insert>(t_name, gt1);
  auto context1 = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  ins->set_transaction_context(context1);
  ins->execute();

  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->row_count(), row_count * 2);
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->chunk_count(),
            2);  // load_table() has finalized first chunk
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->get_chunk(ChunkID{0})->invalid_row_count(), uint32_t{0});
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->get_chunk(ChunkID{1})->invalid_row_count(), uint32_t{0});

  // Rollback Insert - invalidate inserted rows
  context1->rollback(RollbackReason::User);

  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->get_chunk(ChunkID{0})->invalid_row_count(), uint32_t{0});
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(t_name)->get_chunk(ChunkID{1})->invalid_row_count(), uint32_t{3});
}

TEST_F(OperatorsInsertTest, InsertStringNullValue) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  auto table = load_table("resources/test_data/tbl/string_with_null.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto table2 = load_table("resources/test_data/tbl/string_with_null.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 2u);
  EXPECT_EQ(table->row_count(), 8u);

  auto null_val = (*(table->get_chunk(ChunkID{1})->get_segment(ColumnID{0})))[ChunkOffset{2}];
  EXPECT_TRUE(variant_is_null(null_val));
}

TEST_F(OperatorsInsertTest, InsertIntFloatNullValues) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  auto table = load_table("resources/test_data/tbl/int_float_with_null.tbl", ChunkOffset{3});
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto table2 = load_table("resources/test_data/tbl/int_float_with_null.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 4u);
  EXPECT_EQ(table->row_count(), 8u);

  auto null_val_int = (*(table->get_chunk(ChunkID{2})->get_segment(ColumnID{0})))[ChunkOffset{2}];
  EXPECT_TRUE(variant_is_null(null_val_int));

  auto null_val_float = (*(table->get_chunk(ChunkID{2})->get_segment(ColumnID{1})))[ChunkOffset{1}];
  EXPECT_TRUE(variant_is_null(null_val_float));
}

TEST_F(OperatorsInsertTest, InsertNullIntoNonNull) {
  auto table_name = "test1";
  auto table_name2 = "test2";

  auto table = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{3});
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto table2 = load_table("resources/test_data/tbl/int_float_with_null.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name2, table2);

  auto get_table2 = std::make_shared<GetTable>(table_name2);
  get_table2->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table2);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  EXPECT_THROW(insert->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(OperatorsInsertTest, InsertSingleNullFromDummyProjection) {
  auto table_name = "test1";

  auto table = load_table("resources/test_data/tbl/float_with_null.tbl", ChunkOffset{4});
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto dummy_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  dummy_wrapper->execute();

  // 0 + NULL to create an int-NULL
  auto projection = std::make_shared<Projection>(dummy_wrapper, expression_vector(add_(0.0f, null_())));
  projection->execute();

  auto insert = std::make_shared<Insert>(table_name, projection);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_EQ(table->chunk_count(), 2u);
  EXPECT_EQ(table->row_count(), 5u);

  auto null_val = (*(table->get_chunk(ChunkID{1})->get_segment(ColumnID{0})))[ChunkOffset{0}];
  EXPECT_TRUE(variant_is_null(null_val));
}

TEST_F(OperatorsInsertTest, InsertIntoEmptyTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, false);

  const auto target_table =
      std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("target_table", target_table);

  const auto table_int_float = load_table("resources/test_data/tbl/int_float.tbl");

  const auto table_wrapper = std::make_shared<TableWrapper>(table_int_float);
  table_wrapper->execute();

  const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
  auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->commit();

  EXPECT_TABLE_EQ_ORDERED(target_table, table_int_float);
}

TEST_F(OperatorsInsertTest, SetMaxBeginCID) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, false);

  const auto target_table =
      std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("target_table", target_table);

  const auto table_int_float = load_table("resources/test_data/tbl/int_float.tbl");

  const auto table_wrapper = std::make_shared<TableWrapper>(table_int_float);
  table_wrapper->execute();

  const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
  const auto context = std::make_shared<TransactionContext>(TransactionID{1}, CommitID{2}, AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();

  const auto& chunk = target_table->get_chunk(ChunkID{0});
  ASSERT_TRUE(chunk->mvcc_data());
  EXPECT_EQ(chunk->mvcc_data()->max_begin_cid.load(), MvccData::MAX_COMMIT_ID);

  context->commit();

  EXPECT_EQ(chunk->mvcc_data()->max_begin_cid.load(), CommitID{2});
}

// We are the last pending Insert operator for a chunk. Thus, we finalize it on commit/rollback.
TEST_F(OperatorsInsertTest, FinalizeSingleChunk) {
  for (const auto do_commit : {true, false}) {
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
    const auto target_table =
        std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    if (Hyrise::get().storage_manager.has_table("target_table")) {
      Hyrise::get().storage_manager.drop_table("target_table");
    }
    Hyrise::get().storage_manager.add_table("target_table", target_table);

    const auto values_to_insert = std::make_shared<Table>(column_definitions, TableType::Data);
    values_to_insert->append({int32_t{1}});
    values_to_insert->append({int32_t{2}});

    const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
    const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    EXPECT_EQ(target_table->chunk_count(), 0);
    insert->set_transaction_context(transaction_context);
    execute_all({table_wrapper, insert});
    EXPECT_FALSE(insert->execute_failed());
    ASSERT_EQ(target_table->chunk_count(), 1);

    EXPECT_TRUE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 1);
    // Allow Insert to finalize chunk.
    target_table->last_chunk()->mark_as_finalizable();

    if (do_commit) {
      transaction_context->commit();
    } else {
      transaction_context->rollback(RollbackReason::User);
    }

    EXPECT_FALSE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 0);
  }
}

// Multiple Insert operators insert into the same chunk. The operator that finishes last finalizes the chunk on commit/
// rollback.
TEST_F(OperatorsInsertTest, FinalizeSingleChunkMultipleOperators) {
  for (const auto do_commit : {true, false}) {
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
    const auto target_table =
        std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    if (Hyrise::get().storage_manager.has_table("target_table")) {
      Hyrise::get().storage_manager.drop_table("target_table");
    }
    Hyrise::get().storage_manager.add_table("target_table", target_table);

    EXPECT_EQ(target_table->chunk_count(), 0);

    const auto transaction_contexts = std::vector{
        Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No),
        Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No),
    };

    for (auto& transaction_context : transaction_contexts) {
      const auto values_to_insert = std::make_shared<Table>(column_definitions, TableType::Data);
      values_to_insert->append({int32_t{1}});

      const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
      const auto insert = std::make_shared<Insert>("target_table", table_wrapper);

      insert->set_transaction_context(transaction_context);
      execute_all({table_wrapper, insert});
      EXPECT_FALSE(insert->execute_failed());
    }

    ASSERT_EQ(target_table->chunk_count(), 1);
    EXPECT_TRUE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 2);
    // Allow Inserts to finalize chunk.
    target_table->last_chunk()->mark_as_finalizable();

    // The first operator finishes, but it does not finalize the chunk since the second operator is still pending.
    if (do_commit) {
      transaction_contexts[0]->commit();
    } else {
      transaction_contexts[0]->rollback(RollbackReason::User);
    }
    EXPECT_TRUE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 1);

    // The second operator finalizes the chunk once it finishes.
    if (do_commit) {
      transaction_contexts[1]->commit();
    } else {
      transaction_contexts[1]->rollback(RollbackReason::User);
    }
    EXPECT_FALSE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 0);
  }
}

// We insert into multiple chunks, which we finalize on commit/rollback.
TEST_F(OperatorsInsertTest, FinalizeMultipleChunks) {
  for (const auto do_commit : {true, false}) {
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
    const auto target_table =
        std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    if (Hyrise::get().storage_manager.has_table("target_table")) {
      Hyrise::get().storage_manager.drop_table("target_table");
    }
    Hyrise::get().storage_manager.add_table("target_table", target_table);

    const auto values_to_insert = std::make_shared<Table>(column_definitions, TableType::Data);
    for (auto value = int32_t{0}; value < 9; ++value) {
      values_to_insert->append({value});
    }

    const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
    const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    EXPECT_EQ(target_table->chunk_count(), 0);
    insert->set_transaction_context(transaction_context);
    execute_all({table_wrapper, insert});
    EXPECT_FALSE(insert->execute_failed());
    ASSERT_EQ(target_table->chunk_count(), 5);

    for (auto chunk_id = ChunkID{0}; chunk_id < 5; ++chunk_id) {
      EXPECT_TRUE(target_table->get_chunk(chunk_id)->is_mutable());
      EXPECT_EQ(target_table->get_chunk(chunk_id)->mvcc_data()->pending_inserts(), 1);
    }

    if (do_commit) {
      transaction_context->commit();
    } else {
      transaction_context->rollback(RollbackReason::User);
    }

    // The last chunk is not finalized since it is not full.
    EXPECT_TRUE(target_table->last_chunk()->is_mutable());
    EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 0);

    // Previous chunks are finalized by ourselves.
    for (auto chunk_id = ChunkID{0}; chunk_id < 4; ++chunk_id) {
      EXPECT_FALSE(target_table->get_chunk(chunk_id)->is_mutable());
      EXPECT_EQ(target_table->get_chunk(chunk_id)->mvcc_data()->pending_inserts(), 0);
    }
  }
}

// No Insert operator is pending for the last completely filled chunk and we add a new one. Thus, we finalize it
// immediately on execution.
TEST_F(OperatorsInsertTest, FinalizePreviousChunk) {
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
  const auto target_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("target_table", target_table);

  // The first Insert operator creates and fills the first chunk but does not finalize it itself.
  {
    const auto values_to_insert = std::make_shared<Table>(column_definitions, TableType::Data);
    values_to_insert->append({int32_t{1}});
    values_to_insert->append({int32_t{1}});

    const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
    const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    EXPECT_EQ(target_table->chunk_count(), 0);
    insert->set_transaction_context(transaction_context);
    execute_all({table_wrapper, insert});
    EXPECT_FALSE(insert->execute_failed());
    ASSERT_EQ(target_table->chunk_count(), 1);
    transaction_context->commit();
  }

  // The first chunk is full, but it is not finalized yet: No new mutable chunk has been appended yet.
  EXPECT_TRUE(target_table->last_chunk()->is_mutable());
  EXPECT_EQ(target_table->last_chunk()->mvcc_data()->pending_inserts(), 0);

  // The second Insert operator creates the second chunk and immediately finalizes the first chunk.
  {
    const auto values_to_insert = std::make_shared<Table>(column_definitions, TableType::Data);
    values_to_insert->append({int32_t{1}});

    const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
    const auto insert = std::make_shared<Insert>("target_table", table_wrapper);
    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

    EXPECT_EQ(target_table->chunk_count(), 1);
    insert->set_transaction_context(transaction_context);
    execute_all({table_wrapper, insert});
    EXPECT_FALSE(insert->execute_failed());

    // When adding the new chunk, the Insert operator finalizes the previous chunk.
    ASSERT_EQ(target_table->chunk_count(), 2);
    EXPECT_FALSE(target_table->get_chunk(ChunkID{0})->is_mutable());
    EXPECT_TRUE(target_table->get_chunk(ChunkID{1})->is_mutable());
    transaction_context->commit();
  }
}

}  // namespace hyrise
