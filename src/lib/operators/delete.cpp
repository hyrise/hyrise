#include "delete.hpp"

#include "concurrency/transaction_context.hpp"

namespace opossum {

Delete::Delete(const std::shared_ptr<const TableScan>& table_scan) : AbstractModifyingOperator{table_scan} {}

std::shared_ptr<const Table> Delete::on_execute(const TransactionContext* context) {
  const auto reference_table = std::const_pointer_cast<Table>(_input_left->get_output());

  // assumption: table contains only referenced columns that only reference one table
  // assert(col_count > 0)
  // assert(chunk_size == 0)

  const auto chunk = &reference_table->get_chunk(0);

  const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk->get_column(0));

  _pos_list = first_column->pos_list();
  _referenced_table = std::const_pointer_cast<Table>(first_column->referenced_table());
  _tid = context->tid();

  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    // _succeeded = chunk._TIDs[row_id.chunk_offset].compare_exchange_strong(0u, _tid);
    // fallback until replaced by concurrent vector

    auto& row_tid = chunk._TIDs[row_id.chunk_offset];

    if (row_tid != 0u) {
      _succeeded = false;
      return nullptr;
    }

    row_tid = _tid;
  }

  return nullptr;
}

void Delete::commit(const uint32_t cid) {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    chunk._end_CIDs[row_id.chunk_offset] = cid;
  }
}

void Delete::abort() {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);
    auto& row_tid = chunk._TIDs[row_id.chunk_offset];

    if (row_tid != _tid) return;

    row_tid = 0u;
  }
}

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

}  // namespace opossum
