#include "insert.hpp"

#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"

namespace opossum {

Insert::Insert(std::shared_ptr<GetTable> get_table, std::shared_ptr<AbstractOperator> values_to_insert)
    : AbstractModifyingOperator(get_table, values_to_insert) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Insert::on_execute(const TransactionContext* context) {
  auto _table = std::const_pointer_cast<Table>(input_table_left());

  // TODO(all): respect chunk size maybe?
  auto last_chunk_id = _table->chunk_count() - 1;
  auto& last_chunk = _table->get_chunk(last_chunk_id);
  auto& chunk_to_insert = input_table_right()->get_chunk(0);

  // TODO(ALL): RACE CONDITION CAN HAPPEN HERE!!!!!! last chunk could be compressed

  // Lock to get lock.
  size_t new_rows_offset;
  {
    std::lock_guard<std::mutex> lock(*_table->append_mtx);

    new_rows_offset = last_chunk.size();

    last_chunk.set_mvcc_column_size(last_chunk.size() + chunk_to_insert.size(), std::numeric_limits<uint32_t>::max());

    for (size_t column_id = 0; column_id < last_chunk.col_count(); ++column_id) {
      auto _impl = make_unique_by_column_type<AbstractInsertForLoopImpl, InsertForLoopImpl>(
          input_table_left()->column_type(column_id));

      // TODO(ALL): what happens if other threads access columns that havent been resized yet.
      _impl->resize_vector(last_chunk.get_column(column_id), chunk_to_insert.get_column(column_id));
    }
  }

  for (size_t column_id = 0; column_id < last_chunk.col_count(); ++column_id) {
    auto _impl = make_unique_by_column_type<AbstractInsertForLoopImpl, InsertForLoopImpl>(
        input_table_left()->column_type(column_id));

    _impl->move_data(last_chunk.get_column(column_id), chunk_to_insert.get_column(column_id), new_rows_offset);
  }

  for (auto i = 0u; i < chunk_to_insert.size(); i++) {
    last_chunk._TIDs[new_rows_offset + i] = context->tid();
    _inserted_rows.emplace_back(_table->calculate_row_id(last_chunk_id, new_rows_offset + i));
  }

  return nullptr;
}

void Insert::commit(const uint32_t cid) {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _inserted_rows) {
    _table->get_chunk(row_id.chunk_id)._begin_CIDs[row_id.chunk_offset] = cid;
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

void Insert::abort() {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _inserted_rows) {
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

}  // namespace opossum
