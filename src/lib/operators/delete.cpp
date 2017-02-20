#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"

namespace opossum {

Delete::Delete(const std::shared_ptr<const AbstractOperator>& op) : AbstractReadWriteOperator{op} {}

std::shared_ptr<const Table> Delete::on_execute(const TransactionContext* context) {
  const auto reference_table = std::const_pointer_cast<Table>(_input_left->get_output());

  // assumption: table contains only referenced columns that only reference one table
  // assert(col_count > 0)
  // assert(chunk_size == 0)
  // assert(context != nullptr)

  const auto chunk = &reference_table->get_chunk(0);

  const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk->get_column(0));

  _pos_list = first_column->pos_list();
  _referenced_table = std::const_pointer_cast<Table>(first_column->referenced_table());
  _tid = context->transaction_id();

  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    auto expected = 0u;
    _execute_failed = !chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, _tid);

    if (_execute_failed) return nullptr;
  }

  return nullptr;
}

void Delete::commit(const uint32_t cid) {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    chunk.mvcc_columns().end_cids[row_id.chunk_offset] = cid;
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

void Delete::abort() {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    auto expected = _tid;

    // only resets records that have been locked by this operator
    // we don't want to unlock records locked by other transactions
    const auto result = chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, 0u);

    if (!result) return;
  }
}

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

}  // namespace opossum
