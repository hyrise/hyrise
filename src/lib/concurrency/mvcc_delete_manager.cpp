#include "mvcc_delete_manager.hpp"
#include <operators/get_table.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/update.hpp>
#include <operators/validate.hpp>
#include <storage/pos_list.hpp>
#include <storage/reference_segment.hpp>
#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

bool MvccDelete::delete_chunk_logically(const std::string &table_name, ChunkID chunk_id) {
  auto& sm = StorageManager::get();
  const auto table = sm.get_table(table_name);
  auto chunk = table->get_chunk(chunk_id);

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
    return false;
  }

  // TODO(all): Check for success of commit, currently (2019-01-11) not possible.
  transaction_context->commit();

  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());

  return true;
}

bool MvccDelete::delete_chunk_physically(const std::string &tableName, ChunkID chunkID) {
  auto &sm = StorageManager::get();
  auto table = sm.get_table(tableName);
  auto chunk = table->get_chunk(chunkID);

  Assert(chunk, "Chunk does not exist. Physical Delete can not be applied.")
  Assert(chunk->get_cleanup_commit_id() != MvccData::MAX_COMMIT_ID,
          "Chunk needs to be deleted logically before deleting it physically.")


  if(chunk->get_cleanup_commit_id() < TransactionManager::get().get_lowest_active_snapshot_commit_id()) {
    // Release memory, create a "gap" in the chunk vector
    std::vector<std::shared_ptr<Chunk>> chunk_vector = table->chunks();
    chunk_vector[chunkID] = nullptr;
    return true;
  } else {
    // we have to wait with the Physical Delete
    return false;
  }
}

/**
 * Creates a new referencing table with only one chunk from a given table
 */
std::shared_ptr<const Table> MvccDelete::_get_referencing_table(const std::string& table_name,
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

}  // namespace opossum
