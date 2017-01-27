#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>

#include "../concurrency/transaction_context.hpp"
#include "../storage/reference_column.hpp"

namespace opossum {

Validate::Validate(const std::shared_ptr<AbstractOperator> in)
    : AbstractReadOnlyOperator(in), _in_table(in->get_output()), _output(std::make_shared<Table>()) {
  // TODO(EVERYONE): remove _in_table
}

const std::string Validate::name() const { return "Validate"; }

uint8_t Validate::num_in_tables() const { return 1; }

uint8_t Validate::num_out_tables() const { return 1; }

bool is_row_visible(const TransactionContext *context, const Chunk &chunk, uint32_t chunk_offset) {
  const auto our_tid = context->transaction_id();
  const auto our_lcid = context->last_commit_id();
  const auto row_tid = chunk.mvcc_columns().tids[chunk_offset].load();
  const auto begin_cid = chunk.mvcc_columns().begin_cids[chunk_offset];
  const auto end_cid = chunk.mvcc_columns().end_cids[chunk_offset];

  // Taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst
  const auto own_insert = (our_tid == row_tid) && !(our_lcid >= begin_cid) && !(our_lcid >= end_cid);
  const auto past_insert = (our_tid != row_tid) && (our_lcid >= begin_cid) && !(our_lcid >= end_cid);

  return own_insert || past_insert;
}

std::shared_ptr<const Table> Validate::on_execute() { return nullptr; }

std::shared_ptr<const Table> Validate::on_execute(const TransactionContext *transactionContext) {
  auto output = std::make_shared<Table>();

  for (size_t column_id = 0; column_id < _in_table->col_count(); ++column_id) {
    output->add_column(_in_table->column_name(column_id), _in_table->column_type(column_id), false);
  }

  for (ChunkID chunk_id = 0; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = _in_table->get_chunk(chunk_id);
    auto pos_list_out = std::make_shared<PosList>();

    std::shared_ptr<const Table> referenced_table;

    auto ref_col_in = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(0));

    if (ref_col_in) {
      // assumption: validation happens before joins. all columns reference same table.
      referenced_table = ref_col_in->referenced_table();
      for (auto row_id : *ref_col_in->pos_list()) {
        if (is_row_visible(transactionContext, chunk_in, row_id.chunk_offset)) pos_list_out->emplace_back(row_id);
      }
    } else {
      referenced_table = _in_table;
      for (auto i = 0u; i < chunk_in.size(); i++) {
        if (is_row_visible(transactionContext, chunk_in, i)) pos_list_out->emplace_back(RowID{chunk_id, i});
      }
    }

    Chunk chunk_out;
    for (size_t column_id = 0; column_id < _in_table->col_count(); ++column_id) {
      // TODO(everyone): because of projection, column_id might be wrong. need to look it up in case of referencing
      // tables.
      auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, column_id, pos_list_out);
      chunk_out.add_column(ref_col_out);
    }

    output->add_chunk(std::move(chunk_out));
  }
  return output;
}

}  // namespace opossum
