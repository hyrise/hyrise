#include "clustering_sorter.hpp"

#include <memory>
#include <unordered_set>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

ClusteringSorter::ClusteringSorter(const std::shared_ptr<const AbstractOperator>& referencing_table_op, std::shared_ptr<Table> table, const std::set<ChunkID>& chunk_ids, const ColumnID sort_column_id, std::unordered_set<ChunkID>& new_chunk_ids)
    : AbstractReadWriteOperator{OperatorType::ClusteringSorter, referencing_table_op}, _table{table}, _chunk_ids{chunk_ids}, _sort_column_id{sort_column_id}, _new_chunk_ids{new_chunk_ids}, _num_locks{0}, _expected_num_locks{0}, _transaction_id{0} {
      for (const auto chunk_id : _chunk_ids) {
        const auto& chunk = _table->get_chunk(chunk_id);
        Assert(chunk, "chunk disappeared");
        Assert(!chunk->is_mutable(), "ClusteringSorter expects only immutable chunks");
        _expected_num_locks += chunk->size() - chunk->invalid_row_count();
      }
    }

const std::string& ClusteringSorter::name() const {
  static const auto name = std::string{"ClusteringSorter"};
  return name;
}

std::shared_ptr<const Table> ClusteringSorter::_on_execute(std::shared_ptr<TransactionContext> context) {
  _transaction_id = context->transaction_id();

  auto sorting_table = std::make_shared<Table>(_table->column_definitions(), TableType::Data, _table->target_chunk_size(), UseMvcc::Yes);

  std::vector<size_t> invalid_row_counts;
  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    Assert(chunk, "chunk must not be deleted");
    invalid_row_counts.push_back(chunk->invalid_row_count());

    Segments segments;
    for (ColumnID column_id{0}; column_id < _table->column_count(); column_id++) {
      const auto &segment = chunk->get_segment(column_id);
      segments.push_back(segment);
    }

    sorting_table->append_chunk(segments, chunk->mvcc_data());
    sorting_table->last_chunk()->increase_invalid_row_count(chunk->invalid_row_count());
  }


  /* DEBUG
  size_t invalid_rows = 0;
  size_t official_invalid_row_count = 0;
  for (ChunkID chunk_id {0}; chunk_id < sorting_table->chunk_count(); chunk_id++) {
    const auto chunk = sorting_table->get_chunk(chunk_id);
    Assert(chunk, "bug");
    const auto& mvcc_data = chunk->mvcc_data();
    for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
      if (mvcc_data->get_end_cid(offset) != MvccData::MAX_COMMIT_ID) {
        invalid_rows++;
      }
    }
    official_invalid_row_count += chunk->invalid_row_count();
  }
  Assert(invalid_rows >= official_invalid_row_count, "official: " + std::to_string(official_invalid_row_count) + ", actual: " + std::to_string(invalid_rows));
  //*/// DEBUG OVER


  auto wrapper = std::make_shared<TableWrapper>(sorting_table);
  wrapper->execute();

  Assert(transaction_context(), "no transaction_context");
  auto validate = std::make_shared<Validate>(wrapper);
  validate->set_transaction_context(transaction_context());
  validate->execute();
  const auto validate_output_size = validate->get_output()->row_count();
  Assert(validate_output_size <= _expected_num_locks, "Expected validate output to contain at most " + std::to_string(_expected_num_locks) + " rows, but it contains " + std::to_string(validate_output_size) + " rows");

  const std::vector<SortColumnDefinition> sort_column_definitions = { SortColumnDefinition(_sort_column_id, SortMode::Ascending) };
  auto sort = std::make_shared<Sort>(validate, sort_column_definitions, _table->target_chunk_size(), Sort::ForceMaterialization::Yes);
  sort->execute();
  _sorted_table = sort->get_output();

  // get locks for the unsorted chunks in the table
  size_t invalid_row_index = 0;
  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    Assert(chunk, "chunk is not supposed to be deleted");

    const auto success = _lock_chunk(chunk);
    if (!success) {
      std::cout << "Failed locking chunks" << std::endl;
      return nullptr;
    }

    if (chunk->invalid_row_count() != invalid_row_counts[invalid_row_index]) {
      // chunk was modified between sorting and locking
      std::cout << "Invalid chunk count was " << invalid_row_counts[invalid_row_index] << ", but is now " << chunk->invalid_row_count() << std::endl;
      _mark_as_failed();
      return nullptr;
    }

    invalid_row_index++;
  }

  if (_num_locks != _expected_num_locks) {
    // potential race condition: Some row might be invalidated, but the chunk's invalid row counter not yet increased.
    // Thus, comparing the invalid row counters is not sufficient to detect the invalidation, and this operator would unintentionally restore an invalidated row.
    // Fix: a row that is invalidated is also locked - so we can check if we got exactly the expected number of locks
    Assert(_num_locks < _expected_num_locks, "Bug");
    std::cout << "Race condition detected and handled: row was invalidated, but the invalid row counter not yet increased. Expected " << _expected_num_locks  << " locks, but got " << _num_locks << std::endl;
    _mark_as_failed();
    return nullptr;
  }

  Assert(_sorted_table->row_count() == _num_locks, "locked " + std::to_string(_num_locks) + " rows, sorted table size is " + std::to_string(_sorted_table->row_count()) + ". Sorting table size is " + std::to_string(sorting_table->row_count()));
  Assert(_sorted_table->row_count() == _expected_num_locks, "Bug2");

  // no need to get locks for the sorted chunks, as they get inserted as completely new chunks

  return nullptr;
}

void ClusteringSorter::_unlock_all() {
  // we only hold locks for the unsorted chunks
  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    Assert(chunk, "chunk is not supposed to be deleted");
    _unlock_chunk(chunk);
  }

  Assert(_num_locks == 0, "there should be no more locks, but got " + std::to_string(_num_locks));
}

bool ClusteringSorter::_lock_chunk(const std::shared_ptr<Chunk> chunk) {
  Assert(_transaction_id > 0, "_transaction_id not set");
  const auto mvcc_data = chunk->mvcc_data();

  for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
    if (mvcc_data->get_end_cid(offset) != MvccData::MAX_COMMIT_ID) {
      // Row is invalidated. Invalidated rows should already be locked.
      Assert(mvcc_data->get_tid(offset) > 0, "row invalidated, but no tid set");
      continue;
    }

    const auto expected = 0u;
    auto success = mvcc_data->compare_exchange_tid(offset, expected, _transaction_id);
    if (!success) {
      _mark_as_failed();
      return false;
    } else {
      _num_locks++;
    }
  }

  return true;
}

void ClusteringSorter::_unlock_chunk(const std::shared_ptr<Chunk> chunk) {
  const auto mvcc_data = chunk->mvcc_data();

  for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
    if (mvcc_data->get_tid(offset) == _transaction_id) {
      const auto success = mvcc_data->compare_exchange_tid(offset, _transaction_id, 0u);
      Assert(success, "Unable to unlock a row that belongs to our own transaction");
      _num_locks--;
    }
  }
}

void ClusteringSorter::_on_commit_records(const CommitID commit_id) {
  // all locks have been acquired by now

  // MVCC-delete the unsorted chunks
  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    // TODO can this happen?
    Assert(chunk, "chunk disappeared");

    const auto& mvcc_data = chunk->mvcc_data();
    for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
      if (mvcc_data->get_end_cid(offset) == MvccData::MAX_COMMIT_ID) {
        // We assume that nobody inserts rows into the clustering chunks, and that the ClusteringSorter is only executed after all ClusteringPartitioner operators.
        // If those assumptions do not hold, the chunk size might increase during the sort operation, leading to rows we did not lock.
        // Unfortunately, there is not really something as an "insert lock" that stops from inserting. We could simply lock all (i.e., including unused) rows of the old chunk, but is that pretty?
        Assert(mvcc_data->get_tid(offset) == _transaction_id, "Row " + std::to_string(offset) + " was not locked. Did the chunk grow?");
        mvcc_data->set_end_cid(offset, commit_id);
        chunk->increase_invalid_row_count(1);
      }
    }
  }

  // copy the chunks from the sorted table over and update MVCC accordingly
  for (ChunkID chunk_id{0}; chunk_id < _sorted_table->chunk_count(); chunk_id++) {
    const auto& chunk = _sorted_table->get_chunk(chunk_id);
    Assert(chunk, "_sorted_table is not supposed to have removed chunks");

    Segments segments;
    for (ColumnID col_id{0}; col_id < chunk->column_count(); col_id++) {
      const auto& segment = chunk->get_segment(col_id);
      Assert(segment, "segment was null");
      segments.push_back(segment);
    }
    const auto mvcc_data = std::make_shared<MvccData>(chunk->size(), commit_id);

    // transfer meta information
    std::shared_ptr<Chunk> table_chunk;
    {
      const auto append_lock = _table->acquire_append_mutex();
      const auto new_chunk_id = _table->chunk_count();
      _new_chunk_ids.insert(new_chunk_id);
      _table->append_chunk(segments, mvcc_data);
      table_chunk = _table->last_chunk();
      Assert(table_chunk, "Chunk disappeared");
      Hyrise::get().active_chunks_mutex->lock();
      Hyrise::get().active_chunks.insert(new_chunk_id);
      Hyrise::get().active_chunks_mutex->unlock();
    }

    table_chunk->finalize();

    Assert(!chunk->individually_sorted_by().empty(), "chunk has no sorting information");
    table_chunk->set_individually_sorted_by(chunk->individually_sorted_by());
  }

  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    Assert(chunk, "Chunk disappeared");
    chunk->set_cleanup_commit_id(commit_id);
  }

  // _unlock_all();
}

void ClusteringSorter::_on_rollback_records() {
  std::cout << "[ClusteringSorter] performing rollback" << std::endl;
  _unlock_all();
}

std::shared_ptr<AbstractOperator> ClusteringSorter::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  //return std::make_shared<Clustering>(copied_input_left);
  return nullptr;
}

void ClusteringSorter::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
