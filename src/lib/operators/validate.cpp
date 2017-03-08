#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>

#include "../concurrency/transaction_context.hpp"
#include "../storage/reference_column.hpp"

namespace opossum {

namespace {

bool is_row_visible(CommitID our_tid, CommitID our_lcid, ChunkOffset chunk_offset, const Chunk::MvccColumns &columns) {
  const auto row_tid = columns.tids[chunk_offset].load();
  const auto begin_cid = columns.begin_cids[chunk_offset];
  const auto end_cid = columns.end_cids[chunk_offset];

  // Taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst
  // const auto own_insert = (our_tid == row_tid) && !(our_lcid >= begin_cid) && !(our_lcid >= end_cid);
  // const auto past_insert = (our_tid != row_tid) && (our_lcid >= begin_cid) && !(our_lcid >= end_cid);
  // return own_insert || past_insert;

  // since gcc and clang are surprisingly bad at optimizing the above boolean expression, lets do that ourselves
  return our_lcid < end_cid && ((our_lcid >= begin_cid) != (row_tid == our_tid));
}

}  // namespace

Validate::Validate(const std::shared_ptr<AbstractOperator> in)
    : AbstractReadOnlyOperator(in), _in_table(in->get_output()), _output(std::make_shared<Table>()) {}

const std::string Validate::name() const { return "Validate"; }

uint8_t Validate::num_in_tables() const { return 1; }

uint8_t Validate::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Validate::on_execute() {
  throw std::runtime_error("Validate can't be called without a transaction context.");
}

std::shared_ptr<const Table> Validate::on_execute(TransactionContext *transactionContext) {
  auto output = std::make_shared<Table>();

  for (size_t column_id = 0; column_id < _in_table->col_count(); ++column_id) {
    output->add_column(_in_table->column_name(column_id), _in_table->column_type(column_id), false);
  }

  for (ChunkID chunk_id = 0; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = _in_table->get_chunk(chunk_id);
    auto pos_list_out = std::make_shared<PosList>();

    std::shared_ptr<const Table> referenced_table;

    auto ref_col_in = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(0));

    const auto our_tid = transactionContext->transaction_id();
    const auto our_lcid = transactionContext->last_commit_id();
    const auto &mvcc_columns = chunk_in.mvcc_columns();

    if (ref_col_in) {
      // assumption: validation happens before joins. all columns reference same table.
      referenced_table = ref_col_in->referenced_table();
      for (auto row_id : *ref_col_in->pos_list()) {
        if (is_row_visible(our_tid, our_lcid, row_id.chunk_offset, mvcc_columns)) {
          pos_list_out->emplace_back(row_id);
        }
      }
    } else {
      referenced_table = _in_table;
      for (auto i = 0u; i < chunk_in.size(); i++) {
        if (is_row_visible(our_tid, our_lcid, i, mvcc_columns)) {
          pos_list_out->emplace_back(RowID{chunk_id, i});
        }
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
