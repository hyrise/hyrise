#include "delete.hpp"

#include "concurrency/transaction_context.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "utils/atomic_max.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <bool is_single_chunk>
void commit_with_pos_list(const std::shared_ptr<const Table>& referenced_table, const AbstractPosList& pos_list,
                          const CommitID commit_id) {
  // If the PosList references a single chunk (e.g., it is the output of a TableScan), we can (i) resolve the MvccData
  // and (ii) set invalid_row_count and max_end_cid once.
  auto referenced_chunk = std::shared_ptr<const Chunk>{};
  auto mvcc_data = std::shared_ptr<MvccData>{};

  if constexpr (is_single_chunk) {
    referenced_chunk = referenced_table->get_chunk(pos_list.common_chunk_id());
    mvcc_data = referenced_chunk->mvcc_data();
  }

  for (const auto row_id : pos_list) {
    if constexpr (!is_single_chunk) {
      referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);
      mvcc_data = referenced_chunk->mvcc_data();
    }

    // We do not unlock the rows so subsequent transactions properly fail when attempting to update these rows. Thus, we
    // only set_end_cid to our CommitID, but do not set_tid to 0 again.
    mvcc_data->set_end_cid(row_id.chunk_offset, commit_id);

    if constexpr (!is_single_chunk) {
      referenced_chunk->increase_invalid_row_count(ChunkOffset{1});
      set_atomic_max(mvcc_data->max_end_cid, commit_id);
    }
  }

  if constexpr (is_single_chunk) {
    referenced_chunk->increase_invalid_row_count(ChunkOffset{static_cast<ChunkOffset::base_type>(pos_list.size())});
    set_atomic_max(mvcc_data->max_end_cid, commit_id);
  }
}

}  // namespace

namespace hyrise {

Delete::Delete(const std::shared_ptr<const AbstractOperator>& referencing_table_op)
    : AbstractReadWriteOperator{OperatorType::Delete, referencing_table_op}, _transaction_id{0} {}

const std::string& Delete::name() const {
  static const auto name = std::string{"Delete"};
  return name;
}

std::shared_ptr<const Table> Delete::_on_execute(std::shared_ptr<TransactionContext> context) {
  _referencing_table = left_input_table();

  Assert(_referencing_table->type() == TableType::References, "_referencing_table needs to reference another table.");
  Assert(_referencing_table->column_count() > 0, "_referencing_table needs columns to determine referenced table.");

  _transaction_id = context->transaction_id();

  const auto chunk_count = _referencing_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = _referencing_table->get_chunk(chunk_id);

    Assert(chunk->references_exactly_one_table(), "All segments in _referencing_table must reference the same table.");

    const auto first_segment = std::static_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0}));
    const auto pos_list = first_segment->pos_list();

    if constexpr (HYRISE_DEBUG) {
      for (auto column_id = ColumnID{0}; column_id < _referencing_table->column_count(); ++column_id) {
        const auto segment = chunk->get_segment(column_id);
        const auto segment_pos_list = std::dynamic_pointer_cast<const ReferenceSegment>(segment)->pos_list();
        // We could additionally check for `*segment_pos_list == *pos_list`, but for now, comparing pointers is enough.
        Assert(segment_pos_list == pos_list,
               "All segments of a Chunk in _referencing_table must have the same PosList.");
      }
    }

    for (const auto row_id : *pos_list) {
      const auto referenced_chunk = first_segment->referenced_table()->get_chunk(row_id.chunk_id);
      Assert(referenced_chunk, "Referenced chunks are not allowed to be null pointers.");

      const auto& mvcc_data = referenced_chunk->mvcc_data();
      DebugAssert(mvcc_data, "Delete cannot operate on a table without MVCC data.");

      // We validate every row again, which ensures we notice if we try to delete the same row twice within a single
      // statement. This could happen if the input table references the same row multiple times, e.g., due to a UnionAll
      // operator. Though marking a row as deleted multiple times from the same statement/transaction is no problem,
      // multiple deletions currently lead to an incorrect invalid_row_count of the chunk containing the affected row.
      DebugAssert(
          Validate::is_row_visible(
              context->transaction_id(), context->snapshot_commit_id(), mvcc_data->get_tid(row_id.chunk_offset),
              mvcc_data->get_begin_cid(row_id.chunk_offset), mvcc_data->get_end_cid(row_id.chunk_offset)),
          "Trying to delete a row that is not visible to the current transaction. Has the input been validated?");

      // Actual row "lock" for delete happens here, making sure that no other transaction can delete this row.
      const auto expected = TransactionID{0};
      const auto success = mvcc_data->compare_exchange_tid(row_id.chunk_offset, expected, _transaction_id);

      if (!success) {
        // If the row has a set TID, it might be a row that our transaction inserted. No need to compare-and-swap here,
        // because we can only run into conflicts when two transactions try to change this row from the initial TID.
        if (mvcc_data->get_tid(row_id.chunk_offset) == _transaction_id) {
          // Make sure that even we do not see the row anymore.
          mvcc_data->set_tid(row_id.chunk_offset, INVALID_TRANSACTION_ID);
        } else {
          // The row is already locked by someone else and the transaction needs to be rolled back.
          _mark_as_failed();
          return nullptr;
        }
      }
    }
  }

  return nullptr;
}

void Delete::_on_commit_records(const CommitID commit_id) {
  const auto chunk_count = _referencing_table->chunk_count();
  for (auto referencing_chunk_id = ChunkID{0}; referencing_chunk_id < chunk_count; ++referencing_chunk_id) {
    const auto& referencing_chunk = _referencing_table->get_chunk(referencing_chunk_id);
    const auto& referencing_segment =
        static_cast<const ReferenceSegment&>(*referencing_chunk->get_segment(ColumnID{0}));
    const auto referenced_table = referencing_segment.referenced_table();

    const auto& pos_list = *referencing_segment.pos_list();
    if (pos_list.references_single_chunk()) {
      commit_with_pos_list<true>(referenced_table, pos_list, commit_id);
    } else {
      commit_with_pos_list<false>(referenced_table, pos_list, commit_id);
    }
  }
}

void Delete::_on_rollback_records() {
  const auto chunk_count = _referencing_table->chunk_count();
  for (auto referencing_chunk_id = ChunkID{0}; referencing_chunk_id < chunk_count; ++referencing_chunk_id) {
    const auto referencing_chunk = _referencing_table->get_chunk(referencing_chunk_id);
    const auto referencing_segment =
        std::static_pointer_cast<const ReferenceSegment>(referencing_chunk->get_segment(ColumnID{0}));
    const auto referenced_table = referencing_segment->referenced_table();

    for (const auto row_id : *referencing_segment->pos_list()) {
      auto expected = _transaction_id;

      const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

      // unlock all rows locked in _on_execute
      const auto result =
          referenced_chunk->mvcc_data()->compare_exchange_tid(row_id.chunk_offset, expected, TransactionID{0});

      // If the above operation fails, it means the row is locked by another transaction. This must have been
      // the reason why the rollback was initiated. Since _on_execute stopped at this row, we can stop
      // unlocking rows here as well.
      if (!result) {
        return;
      }
    }
  }
}

std::shared_ptr<AbstractOperator> Delete::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Delete>(copied_left_input);
}

void Delete::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
