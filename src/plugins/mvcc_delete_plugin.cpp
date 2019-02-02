#include "mvcc_delete_plugin.hpp"

#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

MvccDeletePlugin::MvccDeletePlugin()
    : _sm(StorageManager::get()),
      _delete_threshold_share_invalidated_rows(0),
      _idle_delay_logical_delete(std::chrono::milliseconds(1000)),
      _idle_delay_physical_delete(std::chrono::milliseconds(1000)) {}

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

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
    const auto& chunks = table->chunks();

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunks.size() - 1; chunk_id++) {
      const auto& chunk = chunks[chunk_id];
      // Only immutable chunks are designated for cleanup
      if (chunk && chunk->get_cleanup_commit_id() == MvccData::MAX_COMMIT_ID) {
        // Evaluate metric
        if (_invalidated_rows_amount(chunk) >= _delete_threshold_share_invalidated_rows) {
          // Trigger logical delete
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
  std::unique_lock<std::mutex> lock(_mutex);

  while (!_physical_delete_queue.empty()) {
    ChunkSpecifier chunk_spec = _physical_delete_queue.front();
    bool success = _delete_chunk_physically(chunk_spec.table_name, chunk_spec.chunk_id);

    if (success) {
      _physical_delete_queue.pop();
    } else
      return;  // wait for more transactions to finish
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
            lock(_mutex);
    _physical_delete_queue.emplace(table_name, chunk_id);
  } else {
    std::cout << "Logical delete of chunk " << chunk_id << " failed." << std::endl;
  }
}

bool MvccDeletePlugin::_delete_chunk_logically(const std::string& table_name, const ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  DebugAssert(chunk != nullptr, "Chunk does not exist. Physical Delete can not be applied.")
      // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
      DebugAssert(chunk_id < (table->chunk_count() - 1),
                  "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

      // Create temporary referencing table that contains the given chunk only
      auto table_filtered = _get_referencing_table(table_name, chunk_id);
  auto table_wrapper = std::make_shared<TableWrapper>(table_filtered);
  table_wrapper->execute();

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(table_wrapper);
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

/**
 * Creates a new referencing table with only one chunk from a given table
 */
std::shared_ptr<const Table> MvccDeletePlugin::_get_referencing_table(const std::string& table_name,
                                                                      const ChunkID chunk_id) {
  auto& sm = StorageManager::get();
  const auto table_in = sm.get_table(table_name);
  const auto chunk_in = table_in->get_chunk(chunk_id);

  // Create new table
  auto table_out = std::make_shared<Table>(table_in->column_definitions(), TableType::References);

  DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(ColumnID{0})),
              "Only Value- or DictionarySegments can be used.");

  // Generate pos_list_out.
  auto pos_list_out = std::make_shared<PosList>();
  auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
  for (auto i = 0u; i < chunk_size; i++) {
    pos_list_out->emplace_back(RowID{chunk_id, i});
  }

  // Create actual ReferenceSegment objects.
  Segments output_segments;
  for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
    auto ref_segment_out = std::make_shared<ReferenceSegment>(table_in, column_id, pos_list_out);
    output_segments.push_back(ref_segment_out);
  }

  if (!pos_list_out->empty() > 0) {
    table_out->append_chunk(output_segments);
  }

  return table_out;
}  // namespace opossum

double MvccDeletePlugin::_invalidated_rows_amount(const std::shared_ptr<Chunk> chunk) const {
  const auto chunk_size = chunk->size();
  const auto end_cids = chunk->mvcc_data()->end_cids;
  CommitID invalid_count = 0;
  for (size_t i = 0; i < chunk_size; i++) {
    if (end_cids[i] < MvccData::MAX_COMMIT_ID) invalid_count++;
  }
  return static_cast<double>(invalid_count) / chunk_size;
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
