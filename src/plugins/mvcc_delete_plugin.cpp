#include "mvcc_delete_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

MvccDeletePlugin::MvccDeletePlugin()
    : _sm(StorageManager::get()),
      _rate_of_invalidated_rows_threshold(0.9),
      _idle_delay_logical_delete(std::chrono::milliseconds(1000)),
      _idle_delay_physical_delete(std::chrono::milliseconds(1000)) {}

const std::string MvccDeletePlugin::description() const { return "Physical MVCC delete plugin"; }

void MvccDeletePlugin::start() {
  _loop_thread_logical_delete = std::make_unique<PausableLoopThread>(_idle_delay_logical_delete,
                                                                     [this](size_t) { this->_logical_delete_loop(); });

  _loop_thread_physical_delete = std::make_unique<PausableLoopThread>(
      _idle_delay_physical_delete, [this](size_t) { this->_physical_delete_loop(); });
}

void MvccDeletePlugin::stop() {
  // Call destructor of PausableLoopThread to terminate its thread
  _loop_thread_logical_delete.reset();
  _loop_thread_physical_delete.reset();
}

/**
 * This function analyzes each chunk of every table and triggers a chunk-cleanup-procedure if a certain threshold of invalidated rows is exceeded.
 */
void MvccDeletePlugin::_logical_delete_loop() {
  for (auto& [table_name, table] : _sm.tables()) {
    // Check all chunks, except for the last one, which is currently used for insertions
    if (table->chunk_count() == 1) return;
    ChunkID max_chunk_id_to_check = static_cast<ChunkID>(table->chunk_count() - 2);

    for (ChunkID chunk_id = ChunkID{0}; chunk_id <= max_chunk_id_to_check; chunk_id++) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (chunk && chunk->get_cleanup_commit_id() == MvccData::MAX_COMMIT_ID) {
        // Calculate metric
        double rate_of_invalidated_rows =
            static_cast<double>(chunk->invalid_row_count()) / static_cast<double>(chunk->size());
        // Evaluate metric
        if (_rate_of_invalidated_rows_threshold <= rate_of_invalidated_rows) {
          _delete_chunk(table_name, chunk_id);
        }
      }
    }  // for each chunk
  }    // for each table
}

/**
 * This function processes the physical-delete-queue until its empty.
 */
void MvccDeletePlugin::_physical_delete_loop() {
  std::unique_lock<std::mutex> lock(_mutex_physical_delete_queue);

  while (!_physical_delete_queue.empty()) {
    ChunkSpecifier chunk_spec = _physical_delete_queue.front();
    bool success = _delete_chunk_physically(chunk_spec.table_name, chunk_spec.chunk_id);

    if (success) {
      _physical_delete_queue.pop();
    } else {
      return;  // wait for more transactions to finish
    }
  }
}

void MvccDeletePlugin::_delete_chunk(const std::string& table_name, const ChunkID chunk_id) {
  // Delete chunk logically
  bool success = _delete_chunk_logically(table_name, chunk_id);

  // Queue physical delete
  if (success) {
    DebugAssert(StorageManager::get().get_table(table_name)->get_chunk(chunk_id)->get_cleanup_commit_id() !=
                    MvccData::MAX_COMMIT_ID,
                "Chunk needs to be deleted logically before deleting it physically.")

        std::unique_lock<std::mutex>
            lock(_mutex_physical_delete_queue);
    _physical_delete_queue.emplace(table_name, chunk_id);
  } else {
    std::cout << "Logical delete of chunk " << chunk_id << " failed." << std::endl;
  }
}

bool MvccDeletePlugin::_delete_chunk_logically(const std::string& table_name, const ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  DebugAssert(chunk != nullptr, "Chunk does not exist. Physical Delete can not be applied.")
      DebugAssert(chunk_id < (table->chunk_count() - 1),
                  "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

      // Create temporary referencing table that contains the given chunk only
      auto transaction_context = TransactionManager::get().new_transaction_context();
  auto gt = std::make_shared<GetTable>(table_name);
  gt->set_transaction_context(transaction_context);

  auto chunk_count = table->chunk_count();
  std::vector<ChunkID> excluded_chunk_ids;
  for (ChunkID excluded_chunk_id = ChunkID{0}; excluded_chunk_id < chunk_count; ++excluded_chunk_id) {
    if (chunk_id != excluded_chunk_id) {
      excluded_chunk_ids.push_back(excluded_chunk_id);
    }
  }

  gt->set_excluded_chunk_ids(excluded_chunk_ids);
  gt->execute();

  // Validate temporary table
  auto validate_table = std::make_shared<Validate>(gt);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use Update operator to delete and re-insert valid records in chunk
  // Pass validate_table into Update operator twice since data will not be changed.
  auto update_table = std::make_shared<Update>(table_name, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if (update_table->execute_failed()) {
    transaction_context->rollback();
    return false;
  } else {
    // TODO(all): Check for success of commit, currently (2019-01-11) not yet possible.
    transaction_context->commit();

    // Mark chunk as logically deleted
    chunk->set_cleanup_commit_id(transaction_context->commit_id());
    std::cout << "Deleted chunk " << chunk_id << " logically." << std::endl;
    return true;
  }
}

bool MvccDeletePlugin::_delete_chunk_physically(const std::string& table_name, const ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);

  DebugAssert(table->get_chunk(chunk_id) != nullptr, "Chunk does not exist. Physical Delete can not be applied.")

      // Check whether there are still active transactions that might use the chunk
      CommitID cleanup_commit_id = table->get_chunk(chunk_id)->get_cleanup_commit_id();
  CommitID lowest_snapshot_commit_id = TransactionManager::get().get_lowest_active_snapshot_commit_id();
  if (cleanup_commit_id < lowest_snapshot_commit_id) {
    DebugAssert(table->chunks()[chunk_id].use_count() == 1,
                "At this point, the chunk should be referenced by the "
                "Table-chunk-vector only.")
        // Usage checks have been passed. Apply physical delete now.
        table->delete_chunk(chunk_id);
    std::cout << "Deleted chunk " << chunk_id << " physically." << std::endl;
    return true;
  } else {
    // Chunk might still be in use. Wait with physical delete.
    return false;
  }
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
