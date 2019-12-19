#include "mvcc_delete_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "Physical MVCC delete plugin"; }

void MvccDeletePlugin::start() {
  _loop_thread_logical_delete =
      std::make_unique<PausableLoopThread>(IDLE_DELAY_LOGICAL_DELETE, [&](size_t) { _logical_delete_loop(); });

  _loop_thread_physical_delete =
      std::make_unique<PausableLoopThread>(IDLE_DELAY_PHYSICAL_DELETE, [&](size_t) { _physical_delete_loop(); });
}

void MvccDeletePlugin::stop() {
  // Call destructor of PausableLoopThread to terminate its thread
  _loop_thread_logical_delete.reset();
  _loop_thread_physical_delete.reset();
  std::queue<TableAndChunkID> empty;
  std::swap(_physical_delete_queue, empty);
}

/**
 * This function analyzes each chunk of every table and triggers a chunk-cleanup-procedure if a certain threshold of
 * invalidated rows is exceeded.
 */
void MvccDeletePlugin::_logical_delete_loop() {
  // Check all tables
  for (auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    if (table->empty() || table->uses_mvcc() != UseMvcc::Yes) continue;

    // Check all chunks, except for the last one, which is currently used for insertions
    const auto max_chunk_id = static_cast<ChunkID>(table->chunk_count() - 1);
    for (auto chunk_id = ChunkID{0}; chunk_id < max_chunk_id; chunk_id++) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (chunk && !chunk->get_cleanup_commit_id()) {
        // Calculate metric 1 – Chunk invalidation level
        const double invalidated_rows_ratio = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        const bool criterion1 = (DELETE_THRESHOLD_PERCENTAGE_INVALIDATED_ROWS <= invalidated_rows_ratio);

        if (!criterion1) {
          continue;
        }

        // Calculate metric 2 – Chunk Hotness
        const CommitID highest_end_commit_id =
            *std::max_element(std::begin(chunk->mvcc_data()->end_cids), std::end(chunk->mvcc_data()->end_cids),
                              [](CommitID a, CommitID b) {
                                // Return the highest end commit id that is actually set (meaning != MAX_COMMIT_ID).
                                if (a == MvccData::MAX_COMMIT_ID) return true;
                                if (b == MvccData::MAX_COMMIT_ID) return false;
                                return a < b;
                              });

        const bool criterion2 =
            highest_end_commit_id + DELETE_THRESHOLD_LAST_COMMIT <= Hyrise::get().transaction_manager.last_commit_id();

        if (!criterion2) {
          continue;
        }

        auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
        const bool success = _try_logical_delete(table_name, chunk_id, transaction_context);

        if (success) {
          DebugAssert(table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                      "Chunk needs to be deleted logically before deleting it physically.");

          std::unique_lock<std::mutex> lock(_mutex_physical_delete_queue);
          _physical_delete_queue.emplace(table, chunk_id);
        }
      }
    }
  }
}

/**
 * This function processes the physical-delete-queue until its empty.
 */
void MvccDeletePlugin::_physical_delete_loop() {
  std::unique_lock<std::mutex> lock(_mutex_physical_delete_queue);

  if (_physical_delete_queue.size()) {
    TableAndChunkID table_and_chunk_id = _physical_delete_queue.front();
    const auto& table = table_and_chunk_id.first;
    const auto& chunk = table->get_chunk(table_and_chunk_id.second);

    DebugAssert(chunk != nullptr, "Chunk does not exist. Physical Delete can not be applied.");

    if (chunk->get_cleanup_commit_id().has_value()) {
      // Check whether there are still active transactions that might use the chunk
      bool conflicting_transactions = false;
      auto lowest_snapshot_commit_id = Hyrise::get().transaction_manager.get_lowest_active_snapshot_commit_id();

      if (lowest_snapshot_commit_id.has_value()) {
        conflicting_transactions = chunk->get_cleanup_commit_id().value() > lowest_snapshot_commit_id.value();
      }

      if (!conflicting_transactions) {
        _delete_chunk_physically(table, table_and_chunk_id.second);
        _physical_delete_queue.pop();
      }
    }
  }
}

bool MvccDeletePlugin::_try_logical_delete(const std::string& table_name, const ChunkID chunk_id,
                                           std::shared_ptr<TransactionContext> transaction_context) {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  Assert(chunk != nullptr, "Chunk does not exist. Logical Delete can not be applied.");
  Assert(chunk_id < (table->chunk_count() - 1),
         "MVCC Logical Delete should not be applied on the last/current mutable chunk.");

  // Create temporary referencing table that contains the given chunk only
  //   Include all ChunksIDs of current table except chunk_id for pruning in GetTable
  std::vector<ChunkID> excluded_chunk_ids(table->chunk_count() - 1);
  std::iota(excluded_chunk_ids.begin(), excluded_chunk_ids.begin() + chunk_id, 0);
  std::iota(excluded_chunk_ids.begin() + chunk_id, excluded_chunk_ids.end(), chunk_id + 1);

  auto get_table = std::make_shared<GetTable>(table_name, excluded_chunk_ids, std::vector<ColumnID>());
  get_table->set_transaction_context(transaction_context);
  get_table->execute();

  // Validate temporary table
  auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(transaction_context);
  validate->execute();

  // Use Update operator to delete and re-insert valid records in chunk
  // Pass validate into Update operator twice since data will not be changed.
  auto update = std::make_shared<Update>(table_name, validate, validate);
  update->set_transaction_context(transaction_context);
  update->execute();

  // Check for success
  if (update->execute_failed()) {
    // Transaction conflict. Usually, the OperatorTask would call rollback, but as we executed Update directly, that is
    // our job.
    transaction_context->rollback();
    return false;
  }

  transaction_context->commit();
  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());
  return true;
}

void MvccDeletePlugin::_delete_chunk_physically(const std::shared_ptr<Table>& table, const ChunkID chunk_id) {
  const auto& chunk = table->get_chunk(chunk_id);

  Assert(chunk->get_cleanup_commit_id().has_value(),
         "The cleanup commit id of the chunk is not set. This should have been done by the logical delete.");

  // Usage checks have been passed. Apply physical delete now.
  table->remove_chunk(chunk_id);
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
