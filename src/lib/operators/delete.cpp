#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "util.hpp"

namespace opossum {

Delete::Delete(const std::shared_ptr<const AbstractOperator>& op) : AbstractReadWriteOperator{op} {}

std::shared_ptr<const Table> Delete::on_execute(TransactionContext* context) {
#ifdef IS_DEBUG
  if (!_execution_input_valid(context)) {
    throw std::runtime_error("Input to Delete isn't valid");
  }
#endif

  const auto& chunk = input_table_left()->get_chunk(0);

  const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk.get_column(0));

  _pos_list = first_column->pos_list();
  _referenced_table = std::const_pointer_cast<Table>(first_column->referenced_table());
  _transaction_id = context->transaction_id();

  for (const auto& row_id : *_pos_list) {
    auto& referenced_chunk = _referenced_table->get_chunk(row_id.chunk_id);

    auto expected = 0u;
    _execute_failed =
        !referenced_chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, _transaction_id);

    // the row is already locked and the transaction needs to be aborted
    if (_execute_failed) return nullptr;
  }

  return nullptr;
}

void Delete::commit(const CommitID cid) {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    chunk.mvcc_columns().end_cids[row_id.chunk_offset] = cid;
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

void Delete::abort() {
  for (const auto& row_id : *_pos_list) {
    auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);

    auto expected = _transaction_id;

    // unlocks all rows locked in on_execute until the operator encountered
    // a row that had already been locked by another transaction
    const auto result = chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, 0u);

    if (!result) return;
  }
}

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

bool Delete::_execution_input_valid(const TransactionContext* context) const {
  if (context == nullptr) return false;

  if (input_table_left()->chunk_count() != 1u) return false;

  const auto& chunk = input_table_left()->get_chunk(0);

  if (chunk.col_count() == 0u) return false;

  if (!chunk_references_only_one_table(chunk)) return false;

  return true;
}

}  // namespace opossum
