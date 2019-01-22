#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"

namespace opossum {

Delete::Delete(const std::string& table_name, const std::shared_ptr<const AbstractOperator>& values_to_delete)
    : AbstractReadWriteOperator{OperatorType::Delete, values_to_delete}, _table_name{table_name}, _transaction_id{0} {}

const std::string Delete::name() const { return "Delete"; }

std::shared_ptr<const Table> Delete::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto values_to_delete = input_table_left();

  DebugAssert(values_to_delete->type() == TableType::References, "values_to_delete needs to reference another table");
  DebugAssert(values_to_delete->column_count() > 0, "values_to_delete needs columns to determine referenced table");

  // If there is nothing to delete, early out. Code below expects there to be Chunks in values_to_delete
  if (values_to_delete->chunk_count() == 0) {
    return nullptr;
  }

  // The first segment of the first Chunk of values_to_delete determines the referenced_table all other
  // ReferenceSegments are expected to reference
  const auto upper_left_reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(
      values_to_delete->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  const auto referenced_table = upper_left_reference_segment->referenced_table();

  context->register_read_write_operator(std::static_pointer_cast<AbstractReadWriteOperator>(shared_from_this()));

  _stored_table = StorageManager::get().get_table(_table_name);
  _transaction_id = context->transaction_id();

  // If values_to_delete references a pruned table (and not the stored table itself), build a mapping from ChunkIDs in
  // the pruned table (`referenced_table`) to the actual ChunkIDs in the stored_table
  auto stored_chunk_id_by_referenced_chunk_id = std::optional<std::vector<ChunkID>>{};
  if (_stored_table != referenced_table) {
    auto stored_chunk_id_by_chunk = std::unordered_map<std::shared_ptr<const Chunk>, ChunkID>{};
    for (auto stored_chunk_id = ChunkID{0}; stored_chunk_id < _stored_table->chunk_count(); ++stored_chunk_id) {
      stored_chunk_id_by_chunk.emplace(_stored_table->get_chunk(stored_chunk_id), stored_chunk_id);
    }

    stored_chunk_id_by_referenced_chunk_id.emplace(referenced_table->chunk_count());
    for (auto referenced_chunk_id = ChunkID{0}; referenced_chunk_id < referenced_table->chunk_count();
         ++referenced_chunk_id) {
      const auto stored_chunk_id = stored_chunk_id_by_chunk.at(referenced_table->get_chunk(referenced_chunk_id));
      (*stored_chunk_id_by_referenced_chunk_id)[referenced_chunk_id] = stored_chunk_id;
    }
  }

  for (ChunkID chunk_id{0}; chunk_id < values_to_delete->chunk_count(); ++chunk_id) {
    const auto chunk = values_to_delete->get_chunk(chunk_id);

    DebugAssert(
        std::dynamic_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0}))->referenced_table() ==
            referenced_table,
        "All segments in values_to_delete must reference the same table");
    DebugAssert(chunk->references_exactly_one_table(),
                "All segments in values_to_delete must reference the same table");

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
                "All segments of a Chunk in values_to_delete must have the same PosList");

    for (auto row_id : *pos_list) {
      // If the `referenced_table` is a pruned version of the stored_table, rewrite the chunk_id to point into the
      // stored table
      if (stored_chunk_id_by_referenced_chunk_id) {
        DebugAssert(row_id.chunk_id < referenced_table->chunk_count(), "RowID out of range");
        row_id.chunk_id = (*stored_chunk_id_by_referenced_chunk_id)[row_id.chunk_id];
      }

      auto referenced_chunk = _stored_table->get_chunk(row_id.chunk_id);

      auto expected = 0u;

      // Scope for the lock on the MVCC data
      {
        auto mvcc_data = referenced_chunk->get_scoped_mvcc_data_lock();
        // Actual row lock for delete happens here
        const auto success = mvcc_data->tids[row_id.chunk_offset].compare_exchange_strong(expected, _transaction_id);

        if (!success) {
          // If the row has a set TID, it might be a row that our TX inserted
          // No need to compare-and-swap here, because we can only run into conflicts when two transactions try to
          // change this row from the initial tid

          if (mvcc_data->tids[row_id.chunk_offset] == _transaction_id) {
            // Make sure that even we don't see it anymore
            mvcc_data->tids[row_id.chunk_offset] = TransactionManager::INVALID_TRANSACTION_ID;
          } else {
            // the row is already locked by someone else and the transaction needs to be rolled back
            _mark_as_failed();
            return nullptr;
          }
        }
      }

      _deleted_rows.emplace_back(row_id);
    }
  }

  return nullptr;
}

void Delete::_on_commit_records(const CommitID cid) {
  for (const auto& row_id : _deleted_rows) {
    auto chunk = _stored_table->get_chunk(row_id.chunk_id);

    chunk->get_scoped_mvcc_data_lock()->end_cids[row_id.chunk_offset] = cid;
    // We do not unlock the rows so subsequent transactions properly fail when attempting to update these rows.
  }
}

void Delete::_finish_commit() {
  const auto table_statistics = _stored_table->table_statistics();
  if (table_statistics) {
    table_statistics->increase_invalid_row_count(_deleted_rows.size());
  }
}

void Delete::_on_rollback_records() {
  for (const auto& row_id : _deleted_rows) {
    auto chunk = _stored_table->get_chunk(row_id.chunk_id);

    auto expected = _transaction_id;

    // unlock all rows locked in _on_execute
    const auto result =
        chunk->get_scoped_mvcc_data_lock()->tids[row_id.chunk_offset].compare_exchange_strong(expected, 0u);

    // If the above operation fails, it means the row is locked by another transaction. This must have been
    // the reason why the rollback was initiated. Since _on_execute stopped at this row, we can stop
    // unlocking rows here as well.
    if (!result) return;
  }
}

std::shared_ptr<AbstractOperator> Delete::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Delete>(_table_name, copied_input_left);
}

void Delete::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
