#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsValidateTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = load_table("resources/test_data/tbl/validate_input.tbl", 2u);
    set_all_records_visible(*_test_table);
    invalidate_record(*_test_table, RowID{ChunkID{1}, 0u}, 2u);

    const auto _test_table2 = load_table("resources/test_data/tbl/int_int3.tbl", 3);

    // Delete Operator works with the Storage Manager, so the test table must also be known to the StorageManager
    Hyrise::get().storage_manager.add_table(_table2_name, _test_table2);

    _gt = std::make_shared<GetTable>(_table2_name);
    _gt->execute();

    _table_wrapper = std::make_shared<TableWrapper>(_test_table);

    _table_wrapper->execute();
  }

  void set_all_records_visible(Table& table);
  void invalidate_record(Table& table, RowID row, CommitID end_cid);

  std::shared_ptr<Table> _test_table;
  std::shared_ptr<TableWrapper> _table_wrapper;
  std::shared_ptr<GetTable> _gt;

  const std::string _table2_name = "table_b";

  static bool forward_is_entire_chunk_visible(std::shared_ptr<Validate> validate,
                                              const std::shared_ptr<const Chunk>& chunk,
                                              const CommitID snapshot_commit_id) {
    return validate->_is_entire_chunk_visible(chunk, snapshot_commit_id);
  }
};

void OperatorsValidateTest::set_all_records_visible(Table& table) {
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    for (auto i = 0u; i < chunk->size(); ++i) {
      mvcc_data->begin_cids[i] = 0u;
      mvcc_data->end_cids[i] = MvccData::MAX_COMMIT_ID;
    }
  }
}

void OperatorsValidateTest::invalidate_record(Table& table, RowID row, CommitID end_cid) {
  auto chunk = table.get_chunk(row.chunk_id);

  chunk->get_scoped_mvcc_data_lock()->end_cids[row.chunk_offset] = end_cid;
  chunk->increase_invalid_row_count(1);
}

TEST_F(OperatorsValidateTest, SimpleValidate) {
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/validate_output_validated.tbl", 2u);

  auto validate = std::make_shared<Validate>(_table_wrapper);
  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);
}

TEST_F(OperatorsValidateTest, ScanValidate) {
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/validate_output_validated_scanned.tbl", 2u);

  auto a = PQPColumnExpression::from_table(*_test_table, "a");
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, greater_than_equals_(a, 2));
  table_scan->set_transaction_context(context);
  table_scan->execute();

  auto validate = std::make_shared<Validate>(table_scan);
  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);
}

TEST_F(OperatorsValidateTest, ValidateAfterDelete) {
  auto t1_context = Hyrise::get().transaction_manager.new_transaction_context();

  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(t1_context);

  validate1->execute();

  EXPECT_EQ(validate1->get_output()->row_count(), 8);
  t1_context->commit();

  auto t2_context = Hyrise::get().transaction_manager.new_transaction_context();

  // Select one row for deletion
  auto table_scan = create_table_scan(_gt, ColumnID{0}, PredicateCondition::Equals, "13");
  table_scan->execute();

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(t2_context);
  delete_op->execute();

  auto validate2 = std::make_shared<Validate>(_gt);
  validate2->set_transaction_context(t2_context);
  validate2->execute();

  EXPECT_EQ(validate2->get_output()->row_count(), 7);
  t2_context->commit();
}

TEST_F(OperatorsValidateTest, ChunkEntirelyVisibleThrowsOnRefChunk) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto snapshot_cid = CommitID{1};
  auto pos_list = std::make_shared<PosList>(std::initializer_list<RowID>({RowID{ChunkID{0}, 0}}));
  Segments segments = {std::make_shared<ReferenceSegment>(_test_table, ColumnID{0}, pos_list)};
  auto chunk = std::make_shared<Chunk>(segments);

  auto validate = std::make_shared<Validate>(nullptr);

  EXPECT_THROW(forward_is_entire_chunk_visible(validate, chunk, snapshot_cid), std::logic_error);
}

TEST_F(OperatorsValidateTest, ChunkNotEntirelyVisibleWithoutMaxBeginCid) {
  auto snapshot_cid = CommitID{1};
  auto vs_int = std::make_shared<ValueSegment<int32_t>>();
  vs_int->append(4);
  auto chunk = std::make_shared<Chunk>(Segments{vs_int}, std::make_shared<MvccData>(1, 0));
  // We explicitly do not finalize the chunk so that max_begin_cid remains emtpy

  auto validate = std::make_shared<Validate>(nullptr);

  EXPECT_FALSE(forward_is_entire_chunk_visible(validate, chunk, snapshot_cid));
}

TEST_F(OperatorsValidateTest, ChunkNotEntirelyVisibleWithLowerSnapshotCid) {
  auto snapshot_cid = CommitID{1};
  auto begin_cid = CommitID{2};
  auto vs_int = std::make_shared<ValueSegment<int32_t>>();
  vs_int->append(4);

  auto chunk = std::make_shared<Chunk>(Segments{vs_int}, std::make_shared<MvccData>(1, begin_cid));
  chunk->finalize();

  auto validate = std::make_shared<Validate>(nullptr);

  EXPECT_FALSE(forward_is_entire_chunk_visible(validate, chunk, snapshot_cid));
}

TEST_F(OperatorsValidateTest, ChunkNotEntirelyVisibleWithInvalidRows) {
  auto snapshot_cid = CommitID{1};
  auto begin_cid = CommitID{0};
  auto vs_int = std::make_shared<ValueSegment<int32_t>>();
  vs_int->append(4);

  auto chunk = std::make_shared<Chunk>(Segments{vs_int}, std::make_shared<MvccData>(1, begin_cid));
  chunk->increase_invalid_row_count(1);
  chunk->finalize();

  auto validate = std::make_shared<Validate>(nullptr);

  EXPECT_FALSE(forward_is_entire_chunk_visible(validate, chunk, snapshot_cid));
}

TEST_F(OperatorsValidateTest, ChunkEntirelyVisible) {
  auto snapshot_cid = CommitID{1};
  auto begin_cid = CommitID{0};
  auto vs_int = std::make_shared<ValueSegment<int32_t>>();
  vs_int->append(4);
  auto chunk = std::make_shared<Chunk>(Segments{vs_int}, std::make_shared<MvccData>(1, begin_cid));
  chunk->finalize();

  auto validate = std::make_shared<Validate>(nullptr);

  EXPECT_TRUE(forward_is_entire_chunk_visible(validate, chunk, snapshot_cid));
}

TEST_F(OperatorsValidateTest, ValidateReferenceSegmentWithMultipleChunks) {
  // If Validate has a reference table as input, it can usually optimize the evaluation of the MVCC data.
  // This optimization is possible, if a PosList of a reference segment references only one chunk.
  // Here, the fallback implementation for a PosList with multiple chunks is tested.

  auto context = std::make_shared<TransactionContext>(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/validate_output_validated.tbl", 2u);

  // Create a PosList referencing more than one chunk
  auto pos_list = std::make_shared<PosList>();
  for (ChunkID chunk_id{0}; chunk_id < _test_table->chunk_count(); ++chunk_id) {
    const auto chunk_size = _test_table->get_chunk(chunk_id)->size();
    for (ChunkOffset chunk_offset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      pos_list->emplace_back(RowID{chunk_id, chunk_offset});
    }
  }

  Segments segments;
  for (ColumnID column_id{0}; column_id < _test_table->column_count(); ++column_id) {
    segments.emplace_back(std::make_shared<ReferenceSegment>(_test_table, column_id, pos_list));
  }

  auto reference_table = std::make_shared<Table>(_test_table->column_definitions(), TableType::References);
  reference_table->append_chunk(segments);

  auto table_wrapper = std::make_shared<TableWrapper>(reference_table);
  table_wrapper->execute();

  auto validate = std::make_shared<Validate>(table_wrapper);
  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);
}

}  // namespace opossum
