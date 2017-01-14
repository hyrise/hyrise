#include "insert.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"

namespace opossum {

Insert::Insert(std::shared_ptr<GetTable> get_table, std::vector<AllTypeVariant>&& values)
    : AbstractModifyingOperator(get_table), _values(values) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Insert::on_execute(const TransactionContext* context) {
  // const auto last_CID = 5;  // from TransactionManager
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  size_t row_id;
  size_t chunk_id;

  // Lock to get actual Table row.
  {
    std::lock_guard<std::mutex> lock(*_table->append_mtx);

    // Append row. Might create new chunk with new MVCC columns
    _table->append(_values);

    // get atomic chunk_id and row_id
    chunk_id = _table->chunk_count() - 1;
    row_id = _table->get_chunk(chunk_id)._TIDs.size() - 1;
  }

  // Set the mvcc column values for chunk:
  auto& chunk = _table->get_chunk(chunk_id);

  // Uncommitted:
  chunk._TIDs[row_id] = context->tid();
  _modified_rows.push_back(_table->calculate_row_id(chunk_id, row_id));

  return nullptr;
}

void Insert::commit(const uint32_t cid) {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _modified_rows) {
    _table->get_chunk(row_id.chunk_id)._begin_CIDs[row_id.chunk_offset] = cid;
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

void Insert::abort() {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _modified_rows) {
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

}  // namespace opossum
