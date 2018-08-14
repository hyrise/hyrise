#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/reference_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {

bool is_row_visible(CommitID our_tid, CommitID snapshot_commit_id, ChunkOffset chunk_offset,
                    const MvccColumns& columns) {
  const auto row_tid = columns.tids[chunk_offset].load();
  const auto begin_cid = columns.begin_cids[chunk_offset];
  const auto end_cid = columns.end_cids[chunk_offset];

  // Taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst
  // auto own_insert = (our_tid == row_tid) && !(snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // auto past_insert = (our_tid != row_tid) && (snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // return own_insert || past_insert;

  // since gcc and clang are surprisingly bad at optimizing the above boolean expression, lets do that ourselves
  return snapshot_commit_id < end_cid && ((snapshot_commit_id >= begin_cid) != (row_tid == our_tid));
}

}  // namespace

Validate::Validate(const std::shared_ptr<AbstractOperator>& in)
    : AbstractReadOnlyOperator(OperatorType::Validate, in) {}

const std::string Validate::name() const { return "Validate"; }

std::shared_ptr<AbstractOperator> Validate::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Validate>(copied_input_left);
}

void Validate::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Validate::_on_execute() {
  Fail("Validate can't be called without a transaction context.");
}

std::shared_ptr<const Table> Validate::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context != nullptr, "Validate requires a valid TransactionContext.");

  const auto in_table = input_table_left();
  auto output = std::make_shared<Table>(in_table->column_definitions(), TableType::References);

  const auto our_tid = transaction_context->transaction_id();
  const auto snapshot_commit_id = transaction_context->snapshot_commit_id();

  for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    const auto chunk_in = in_table->get_chunk(chunk_id);

    ChunkColumns output_columns;
    auto pos_list_out = std::make_shared<PosList>();
    auto referenced_table = std::shared_ptr<const Table>();
    const auto ref_col_in = std::dynamic_pointer_cast<const ReferenceColumn>(chunk_in->get_column(ColumnID{0}));

    // If the columns in this chunk reference a column, build a poslist for a reference column.
    if (ref_col_in) {
      DebugAssert(chunk_in->references_exactly_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");

      // Check all rows in the old poslist and put them in pos_list_out if they are visible.
      referenced_table = ref_col_in->referenced_table();
      DebugAssert(referenced_table->has_mvcc(), "Trying to use Validate on a table that has no MVCC columns");

      for (auto row_id : *ref_col_in->pos_list()) {
        const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

        auto mvcc_columns = referenced_chunk->get_scoped_mvcc_columns_lock();

        if (is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_columns)) {
          pos_list_out->emplace_back(row_id);
        }
      }

      // Construct the actual ReferenceColumn objects and add them to the chunk.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        const auto column = std::static_pointer_cast<const ReferenceColumn>(chunk_in->get_column(column_id));
        const auto referenced_column_id = column->referenced_column_id();
        auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, referenced_column_id, pos_list_out);
        output_columns.push_back(ref_col_out);
      }

      // Otherwise we have a Value- or DictionaryColumn and simply iterate over all rows to build a poslist.
    } else {
      referenced_table = in_table;
      DebugAssert(chunk_in->has_mvcc_columns(), "Trying to use Validate on a table that has no MVCC columns");
      const auto mvcc_columns = chunk_in->get_scoped_mvcc_columns_lock();

      // Generate pos_list_out.
      auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
      for (auto i = 0u; i < chunk_size; i++) {
        if (is_row_visible(our_tid, snapshot_commit_id, i, *mvcc_columns)) {
          pos_list_out->emplace_back(RowID{chunk_id, i});
        }
      }

      // Create actual ReferenceColumn objects.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, column_id, pos_list_out);
        output_columns.push_back(ref_col_out);
      }
    }

    if (!pos_list_out->empty() > 0) {
      output->append_chunk(output_columns);
    }
  }
  return output;
}

}  // namespace opossum
