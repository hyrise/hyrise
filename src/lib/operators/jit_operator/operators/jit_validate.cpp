#include "jit_validate.hpp" // NEEDEDINCLUDE // NEEDEDINCLUDE

#include "operators/validate.hpp" // NEEDEDINCLUDE
#include "storage/mvcc_data.hpp" // NEEDEDINCLUDE

namespace opossum {

namespace {

bool is_row_visible(const CommitID our_tid, const TransactionID row_tid, const CommitID snapshot_commit_id,
                    const ChunkOffset chunk_offset, const MvccData& mvcc_data) {
  const auto begin_cid = mvcc_data.begin_cids[chunk_offset];
  const auto end_cid = mvcc_data.end_cids[chunk_offset];
  return Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid);
}

}  // namespace

JitValidate::JitValidate(const TableType input_table_type) : _input_table_type(input_table_type) {}

std::string JitValidate::description() const { return "[Validate]"; }

void JitValidate::set_input_table_type(const TableType input_table_type) { _input_table_type = input_table_type; }

void JitValidate::_consume(JitRuntimeContext& context) const {
  if (_input_table_type == TableType::References) {
    const auto row_id = (*context.pos_list)[context.chunk_offset];
    const auto& referenced_chunk = context.referenced_table->get_chunk(row_id.chunk_id);
    const auto mvcc_data = referenced_chunk->get_scoped_mvcc_data_lock();
    const auto row_tid = _load_atomic_value(mvcc_data->tids[row_id.chunk_offset]);
    if (is_row_visible(context.transaction_id, row_tid, context.snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
      _emit(context);
    }
  } else {
    const auto row_tid = context.row_tids[context.chunk_offset];
    if (is_row_visible(context.transaction_id, row_tid, context.snapshot_commit_id, context.chunk_offset,
                       *context.mvcc_data)) {
      _emit(context);
    }
  }
}

TransactionID JitValidate::_load_atomic_value(const copyable_atomic<TransactionID>& transaction_id) {
  return transaction_id.load();
}

}  // namespace opossum
