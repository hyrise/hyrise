#include "insert.hpp"

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

Insert::Insert(std::string table_name, std::shared_ptr<AbstractOperator> values_to_insert)
    : AbstractReadWriteOperator(values_to_insert), _table_name(table_name) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Insert::on_execute(TransactionContext* context) {
  _table = StorageManager::get().get_table(_table_name);

  // TODO(all): respect chunk size maybe?
  auto last_chunk_id = _table->chunk_count() - 1;
  auto& last_chunk = _table->get_chunk(last_chunk_id);
  auto& chunk_to_insert = input_table_left()->get_chunk(0);
  auto num_rows_to_insert = chunk_to_insert.size();

  // TODO(ALL): RACE CONDITION CAN HAPPEN HERE!!!!!! last chunk could be compressed

  auto typed_column_processors = std::vector<std::unique_ptr<AbstractTypedColumnProcessor>>();
  for (size_t column_id = 0; column_id < last_chunk.col_count(); ++column_id) {
    typed_column_processors.emplace_back(
        make_unique_by_column_type<AbstractTypedColumnProcessor, TypedColumnProcessor>(_table->column_type(column_id)));
  }

  // Lock to get lock.
  size_t new_rows_offset;
  {
    std::lock_guard<std::mutex> lock(*_table->append_mtx);

    new_rows_offset = last_chunk.size();

    last_chunk.set_mvcc_column_size(last_chunk.size() + num_rows_to_insert, std::numeric_limits<uint32_t>::max());

    for (size_t i = 0; i < last_chunk.col_count(); ++i) {
      // TODO(ALL): what happens if other threads access columns that havent been resized yet.
      typed_column_processors[i]->resize_vector(last_chunk.get_column(i), num_rows_to_insert);
    }
  }

  for (size_t i = 0; i < last_chunk.col_count(); ++i) {
    typed_column_processors[i]->move_data(last_chunk.get_column(i), chunk_to_insert.get_column(i), num_rows_to_insert,
                                          new_rows_offset);
  }

  for (auto i = 0u; i < num_rows_to_insert; i++) {
    last_chunk.mvcc_columns().tids[new_rows_offset + i] = context->transaction_id();
    _inserted_rows.emplace_back(_table->calculate_row_id(last_chunk_id, new_rows_offset + i));
  }

  return nullptr;
}

void Insert::commit(const uint32_t cid) {
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
