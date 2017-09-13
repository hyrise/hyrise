#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>

#include "concurrency/transaction_context.hpp"
#include "storage/reference_column.hpp"
#include "utils/assert.hpp"

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

Validate::Validate(const std::shared_ptr<AbstractOperator> in) : AbstractReadOnlyOperator(in) {}

const std::string Validate::name() const { return "Validate"; }

uint8_t Validate::num_in_tables() const { return 1; }

uint8_t Validate::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Validate::_on_execute() {
  Fail("Validate can't be called without a transaction context.");
  return {};
}

std::shared_ptr<const Table> Validate::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context != nullptr, "Validate requires a valid TransactionContext.");

  const auto _in_table = _input_table_left();
  auto output = std::make_shared<Table>();

  // Save column structure.
  for (ColumnID column_id{0}; column_id < _in_table->col_count(); ++column_id) {
    output->add_column_definition(_in_table->column_name(column_id), _in_table->column_type(column_id));
  }

  const auto our_tid = transaction_context->transaction_id();
  const auto our_lcid = transaction_context->last_commit_id();

  for (ChunkID chunk_id{0}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    const auto &chunk_in = _in_table->get_chunk(chunk_id);

    auto chunk_out = Chunk{};
    auto pos_list_out = std::make_shared<PosList>();
    auto referenced_table = std::shared_ptr<const Table>();
    const auto ref_col_in = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(ColumnID{0}));

    // If the columns in this chunk reference a column, build a poslist for a reference column.
    if (ref_col_in) {
      DebugAssert(chunk_in.references_only_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");

      // Check all rows in the old poslist and put them in pos_list_out if they are visible.
      referenced_table = ref_col_in->referenced_table();
      for (auto row_id : *ref_col_in->pos_list()) {
        const auto &referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

        auto mvcc_columns = referenced_chunk.mvcc_columns();
        if (is_row_visible(our_tid, our_lcid, row_id.chunk_offset, *mvcc_columns)) {
          pos_list_out->emplace_back(row_id);
        }
      }

      // Construct the actual ReferenceColumn objects and add them to the chunk.
      for (ColumnID column_id{0}; column_id < chunk_in.col_count(); ++column_id) {
        const auto column = std::static_pointer_cast<ReferenceColumn>(chunk_in.get_column(column_id));
        const auto referenced_column_id = column->referenced_column_id();
        auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, referenced_column_id, pos_list_out);
        chunk_out.add_column(ref_col_out);
      }

      // Otherwise we have a Value- or DictionaryColumn and simply iterate over all rows to build a poslist.
    } else {
      referenced_table = _in_table;
      const auto mvcc_columns = chunk_in.mvcc_columns();

      // Generate pos_list_out.
      for (auto i = 0u; i < chunk_in.size(); i++) {
        if (is_row_visible(our_tid, our_lcid, i, *mvcc_columns)) {
          pos_list_out->emplace_back(RowID{chunk_id, i});
        }
      }

      // Create actual ReferenceColumn objects.
      for (ColumnID column_id{0}; column_id < chunk_in.col_count(); ++column_id) {
        auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, column_id, pos_list_out);
        chunk_out.add_column(ref_col_out);
      }
    }

    output->add_chunk(std::move(chunk_out));
  }
  return output;
}

}  // namespace opossum
