#include "insert.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

Insert::Insert(const std::string& table_name, const std::shared_ptr<AbstractOperator>& values_to_insert)
    : AbstractReadWriteOperator(values_to_insert), _table_name(table_name) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Insert::on_execute(TransactionContext* context) {
#ifdef IS_DEBUG
  if (input_table_left()->chunk_count() != 1) {
    throw std::runtime_error("Input to Insert isn't valid: Number of chunks is not 1.");
  }
#endif
  _table = StorageManager::get().get_table(_table_name);

  // these TypedColumnProcessors kind of retrieve the template parameter of the columns.
  auto typed_column_processors = std::vector<std::unique_ptr<AbstractTypedColumnProcessor>>();
  for (auto column_id = 0u; column_id < _table->get_chunk(0).col_count(); ++column_id) {
    typed_column_processors.emplace_back(
        make_unique_by_column_type<AbstractTypedColumnProcessor, TypedColumnProcessor>(_table->column_type(column_id)));
  }

  auto& chunk_to_insert = input_table_left()->get_chunk(0);
  auto num_rows_to_insert = chunk_to_insert.size();

  // First, allocate space for all the rows to insert. Do so while locking the table
  // to prevent multiple threads modifying the table's size simultaneously.
  auto start_index = 0u;
  auto start_chunk_id = 0u;
  auto total_chunks_inserted = 0u;
  {
    auto lock = _table->acquire_append_mutex();

    start_chunk_id = _table->chunk_count() - 1;
    auto& last_chunk = _table->get_chunk(start_chunk_id);
    start_index = last_chunk.size();

    auto remaining_rows = num_rows_to_insert;
    while (remaining_rows > 0) {
      auto& current_chunk = _table->get_chunk(_table->chunk_count() - 1);
      auto rows_to_insert_this_loop = std::min(_table->chunk_size() - current_chunk.size(), remaining_rows);

      // Resize MVCC vectors.
      current_chunk.set_mvcc_column_size(current_chunk.size() + rows_to_insert_this_loop, Chunk::MAX_COMMIT_ID);

      // Resize current chunk to full size.
      for (auto i = 0u; i < current_chunk.col_count(); ++i) {
        typed_column_processors[i]->resize_vector(current_chunk.get_column(i),
                                                  current_chunk.size() + rows_to_insert_this_loop);
      }

      remaining_rows -= rows_to_insert_this_loop;

      // Create new chunk if necessary.
      if (remaining_rows > 0) {
        _table->create_new_chunk();
        total_chunks_inserted++;
      }
    }
  }
  // TODO(all): make compress chunk thread-safe; if it gets called here by another thread, things will likely break.

  // Then, actually insert the data.
  auto input_offset = 0u;

  for (auto chunk_id = start_chunk_id; chunk_id <= start_chunk_id + total_chunks_inserted; chunk_id++) {
    auto end_index = std::min(_table->get_chunk(chunk_id).size(), start_index + (num_rows_to_insert - input_offset));

    auto& current_chunk = _table->get_chunk(chunk_id);
    for (auto i = 0u; i < current_chunk.col_count(); ++i) {
      typed_column_processors[i]->copy_data(current_chunk.get_column(i), start_index, end_index,
                                            chunk_to_insert.get_column(i), input_offset);
    }

    for (auto i = start_index; i < end_index; i++) {
      // we do not need to check whether other operators have locked the rows, we have just created them
      // and they are not visible for other operators
      current_chunk.mvcc_columns().tids[i] = context->transaction_id();
      _inserted_rows.emplace_back(_table->calculate_row_id(chunk_id, i));
    }

    input_offset += end_index - start_index;
    start_index = 0u;
  }

  return nullptr;
}

void Insert::commit(const CommitID cid) {
  for (auto row_id : _inserted_rows) {
    auto& chunk = _table->get_chunk(row_id.chunk_id);

    chunk.mvcc_columns().begin_cids[row_id.chunk_offset] = cid;
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

void Insert::abort() {
  for (auto row_id : _inserted_rows) {
    auto& chunk = _table->get_chunk(row_id.chunk_id);
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

}  // namespace opossum
