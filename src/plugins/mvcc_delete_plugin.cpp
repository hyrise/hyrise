#include "mvcc_delete_plugin.hpp"

#include <cstddef>
#include <iomanip>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/assert.hpp"
#include "utils/log_manager.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

std::string MvccDeletePlugin::description() const {
  return "Physical MVCC delete plugin";
}

void MvccDeletePlugin::start() {
  _loop_thread_logical_delete = std::make_unique<PausableLoopThread>(IDLE_DELAY_LOGICAL_DELETE, [&](size_t /*unused*/) {
    _logical_delete_loop();
  });

  _loop_thread_physical_delete =
      std::make_unique<PausableLoopThread>(IDLE_DELAY_PHYSICAL_DELETE, [&](size_t /*unused*/) {
        _physical_delete_loop();
      });
}

void MvccDeletePlugin::stop() {
  // Call destructor of PausableLoopThread to terminate its thread
  _loop_thread_logical_delete.reset();
  _loop_thread_physical_delete.reset();
  _physical_delete_queue = {};
}

/**
 * This function analyzes each chunk of every table and triggers a chunk-cleanup-procedure if a certain threshold of
 * invalidated rows is exceeded.
 */
void MvccDeletePlugin::_logical_delete_loop() {
  const auto tables = Hyrise::get().storage_manager.tables();

  // Check all tables
  for (const auto& [table_name, table] : tables) {
    if (table->empty() || table->uses_mvcc() != UseMvcc::Yes) {
      continue;
    }
    auto saved_memory = size_t{0};
    auto num_chunks = size_t{0};

    // Check all chunks, except for the last one, which is currently used for insertions
    const auto max_chunk_id = static_cast<ChunkID>(table->chunk_count() - 1);
    for (auto chunk_id = ChunkID{0}; chunk_id < max_chunk_id; ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (chunk && !chunk->get_cleanup_commit_id()) {
        const auto chunk_memory = chunk->memory_usage(MemoryUsageCalculationMode::Sampled);

        // Calculate metric 1 – Chunk invalidation level
        const auto invalidated_rows_ratio = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        const auto criterion1 = (DELETE_THRESHOLD_PERCENTAGE_INVALIDATED_ROWS <= invalidated_rows_ratio);

        if (!criterion1) {
          continue;
        }

        // Calculate metric 2 – Chunk Hotness
        auto highest_end_commit_id = CommitID{0};
        const auto chunk_size = chunk->size();
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
          const auto commit_id = chunk->mvcc_data()->get_end_cid(chunk_offset);
          if (commit_id != MAX_COMMIT_ID && commit_id > highest_end_commit_id) {
            highest_end_commit_id = commit_id;
          }
        }

        const auto criterion2 =
            highest_end_commit_id + DELETE_THRESHOLD_LAST_COMMIT <= Hyrise::get().transaction_manager.last_commit_id();

        if (!criterion2) {
          continue;
        }

        auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
        const auto success = _try_logical_delete(table_name, chunk_id, transaction_context);

        if (success) {
          DebugAssert(table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                      "Chunk needs to be deleted logically before deleting it physically.");

          const auto lock = std::lock_guard<std::mutex>{_physical_delete_queue_mutex};
          _physical_delete_queue.emplace(table, chunk_id);
          saved_memory += chunk_memory;
          ++num_chunks;
        }
      }
    }
    if (saved_memory > 0) {
      auto message = std::ostringstream{};
      const auto saved_mb = static_cast<float>(saved_memory) / (1000.0f * 1000.0f);
      message << "Consolidated " << num_chunks << " chunk(s) of " << table_name << ", saved approx. "
              << std::setprecision(2) << saved_mb << " MB";
      Hyrise::get().log_manager.add_message("MvccDeletePlugin", message.str(), LogLevel::Info);
    }
  }
}

/**
 * This function processes the physical-delete-queue until its empty.
 */
void MvccDeletePlugin::_physical_delete_loop() {
  const auto lock = std::lock_guard<std::mutex>{_physical_delete_queue_mutex};

  if (!_physical_delete_queue.empty()) {
    const auto& [table, chunk_id] = _physical_delete_queue.front();
    const auto& chunk = table->get_chunk(chunk_id);

    DebugAssert(chunk, "Chunk does not exist. Physical Delete can not be applied.");

    if (chunk->get_cleanup_commit_id()) {
      // Check whether there are still active transactions that might use the chunk.
      auto transactions_conflict = false;
      const auto lowest_snapshot_commit_id = Hyrise::get().transaction_manager.get_lowest_active_snapshot_commit_id();

      if (lowest_snapshot_commit_id) {
        transactions_conflict = *chunk->get_cleanup_commit_id() > *lowest_snapshot_commit_id;
      }

      if (!transactions_conflict) {
        _delete_chunk_physically(table, chunk_id);
        _physical_delete_queue.pop();
      }
    }
  }
}

bool MvccDeletePlugin::_try_logical_delete(const std::string& table_name, const ChunkID chunk_id,
                                           const std::shared_ptr<TransactionContext>& transaction_context) {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  Assert(chunk, "Chunk does not exist. Logical Delete can not be applied.");
  Assert(chunk_id < (table->chunk_count() - 1),
         "MVCC Logical Delete should not be applied on the last/current mutable chunk.");

  // Create temporary referencing table that contains the given chunk only
  //   Include all ChunksIDs of current table except chunk_id for pruning in GetTable
  auto excluded_chunk_ids = std::vector<ChunkID>(table->chunk_count() - 1);
  std::iota(excluded_chunk_ids.begin(), excluded_chunk_ids.begin() + chunk_id, 0);
  std::iota(excluded_chunk_ids.begin() + chunk_id, excluded_chunk_ids.end(), chunk_id + 1);

  const auto get_table = std::make_shared<GetTable>(table_name, excluded_chunk_ids, std::vector<ColumnID>());
  get_table->set_transaction_context(transaction_context);
  get_table->execute();

  // Validate temporary table
  const auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(transaction_context);
  validate->execute();

  // Use Update operator to delete and re-insert valid records in chunk
  // Pass validate into Update operator twice since data will not be changed.
  const auto update = std::make_shared<Update>(table_name, validate, validate);
  update->set_transaction_context(transaction_context);
  update->execute();

  // Check for success
  if (update->execute_failed()) {
    // Transaction conflict. Usually, the OperatorTask would call rollback, but as we executed Update directly, that is
    // our job.
    transaction_context->rollback(RollbackReason::Conflict);
    return false;
  }

  transaction_context->commit();
  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());
  return true;
}

void MvccDeletePlugin::_delete_chunk_physically(const std::shared_ptr<Table>& table, const ChunkID chunk_id) {
  const auto& chunk = table->get_chunk(chunk_id);

  Assert(chunk->get_cleanup_commit_id(),
         "The cleanup commit id of the chunk is not set. This should have been done by the logical delete.");

  // Usage checks have been passed. Apply physical delete now.
  table->remove_chunk(chunk_id);
}

EXPORT_PLUGIN(MvccDeletePlugin);

}  // namespace hyrise
