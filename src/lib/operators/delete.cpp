#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

Delete::Delete(const std::shared_ptr<const AbstractOperator>& referencing_table_op)
    : AbstractReadWriteOperator{OperatorType::Delete, referencing_table_op}, _transaction_id{0} {}

const std::string& Delete::name() const {
  static const auto name = std::string{"Delete"};
  return name;
}

std::shared_ptr<const Table> Delete::_on_execute(std::shared_ptr<TransactionContext> context) {
  _referencing_table = input_table_left();

  DebugAssert(_referencing_table->type() == TableType::References,
              "_referencing_table needs to reference another table");
  DebugAssert(_referencing_table->column_count() > 0, "_referencing_table needs columns to determine referenced table");

  _transaction_id = context->transaction_id();

  for (ChunkID chunk_id{0}; chunk_id < _referencing_table->chunk_count(); ++chunk_id) {
    const auto chunk = _referencing_table->get_chunk(chunk_id);

    DebugAssert(chunk->references_exactly_one_table(),
                "All segments in _referencing_table must reference the same table");

    const auto first_segment = std::static_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0}));
    const auto pos_list = first_segment->pos_list();

    DebugAssert(std::all_of(chunk->segments().begin(), chunk->segments().end(),
                            [&](const auto& segment) {
                              const auto segment_pos_list =
                                  std::dynamic_pointer_cast<const ReferenceSegment>(segment)->pos_list();
                              // We could additionally check for `*segment_pos_list == *pos_list`, but atm comparing
                              // pointers is sufficient
                              return segment_pos_list == pos_list;
                            }),
                "All segments of a Chunk in _referencing_table must have the same PosList");

    for (auto row_id : *pos_list) {
      const auto referenced_chunk = first_segment->referenced_table()->get_chunk(row_id.chunk_id);
      Assert(referenced_chunk, "Referenced chunks are not allowed to be null pointers");

      // Scope for the lock on the MVCC data
      {
        auto mvcc_data = referenced_chunk->get_scoped_mvcc_data_lock();

        DebugAssert(
            Validate::is_row_visible(context->transaction_id(), context->snapshot_commit_id(),
                                     mvcc_data->tids[row_id.chunk_offset], mvcc_data->begin_cids[row_id.chunk_offset],
                                     mvcc_data->end_cids[row_id.chunk_offset]),
            "Trying to delete a row that is not visible to the current transaction. Has the input been validated?");

        // Actual row "lock" for delete happens here, making sure that no other transaction can delete this row
        auto expected = 0u;
        const auto success = mvcc_data->tids[row_id.chunk_offset].compare_exchange_strong(expected, _transaction_id);

        if (!success) {
          // If the row has a set TID, it might be a row that our TX inserted
          // No need to compare-and-swap here, because we can only run into conflicts when two transactions try to
          // change this row from the initial tid

          if (mvcc_data->tids[row_id.chunk_offset] == _transaction_id) {
            // Make sure that even we don't see it anymore
            mvcc_data->tids[row_id.chunk_offset] = INVALID_TRANSACTION_ID;
          } else {
            // the row is already locked by someone else and the transaction needs to be rolled back
            _mark_as_failed();
            return nullptr;
          }
        }
      }
    }
  }

  return nullptr;
}

void Delete::_on_commit_records(const CommitID cid) {
  for (ChunkID referencing_chunk_id{0}; referencing_chunk_id < _referencing_table->chunk_count();
       ++referencing_chunk_id) {
    const auto referencing_chunk = _referencing_table->get_chunk(referencing_chunk_id);
    const auto referencing_segment =
        std::static_pointer_cast<const ReferenceSegment>(referencing_chunk->get_segment(ColumnID{0}));
    const auto referenced_table = referencing_segment->referenced_table();

    for (const auto& row_id : *referencing_segment->pos_list()) {
      const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

      referenced_chunk->get_scoped_mvcc_data_lock()->end_cids[row_id.chunk_offset] = cid;
      referenced_chunk->increase_invalid_row_count(1);
      // We do not unlock the rows so subsequent transactions properly fail when attempting to update these rows.
    }
  }
}

void Delete::_on_rollback_records() {
  for (ChunkID referencing_chunk_id{0}; referencing_chunk_id < _referencing_table->chunk_count();
       ++referencing_chunk_id) {
    const auto referencing_chunk = _referencing_table->get_chunk(referencing_chunk_id);
    const auto referencing_segment =
        std::static_pointer_cast<const ReferenceSegment>(referencing_chunk->get_segment(ColumnID{0}));
    const auto referenced_table = referencing_segment->referenced_table();

    for (const auto& row_id : *referencing_segment->pos_list()) {
      auto expected = _transaction_id;

      const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

      // unlock all rows locked in _on_execute
      const auto result =
          referenced_chunk->get_scoped_mvcc_data_lock()->tids[row_id.chunk_offset].compare_exchange_strong(expected,
                                                                                                           0u);

      // If the above operation fails, it means the row is locked by another transaction. This must have been
      // the reason why the rollback was initiated. Since _on_execute stopped at this row, we can stop
      // unlocking rows here as well.
      if (!result) return;
    }
  }
}

std::shared_ptr<AbstractOperator> Delete::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Delete>(copied_input_left);
}

void Delete::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
