#include <operators/get_table.hpp>
#include <operators/validate.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/update.hpp>
#include "mvcc_delete_manager.hpp"
#include "storage/storage_manager.hpp"
#include "concurrency/transaction_manager.hpp"


namespace opossum {

void MvccDeleteManager::run_logical_delete(const std::string &table_name, ChunkID chunk_id) {
  _delete_logically(table_name, chunk_id);
}

bool MvccDeleteManager::_delete_logically(const std::string &table_name, const ChunkID chunk_id) {
  auto& sm = StorageManager::get();
  const auto table = sm.get_table(table_name);
  auto chunk = table->get_chunk(chunk_id);

  // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
  DebugAssert(chunk_id < (table->chunk_count() - 1), "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

  auto get_table = std::make_shared<GetTable>(table_name);
  std::vector<ChunkID> excluded_chunks;
  excluded_chunks.reserve(table->chunk_count());
  for(ChunkID current_chunk_id{0}; current_chunk_id < table->chunk_count(); ++current_chunk_id) {
    if(current_chunk_id != chunk_id)
    excluded_chunks.push_back(current_chunk_id);
  }
  get_table->set_excluded_chunk_ids(excluded_chunks);
  get_table->execute();

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use UPDATE operator to DELETE and RE-INSERT valid records in chunk
    // Pass validate_table as input twice since data will not be changed.
  auto update_table = std::make_shared<Update>(table_name, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if(update_table->execute_failed()) {
    return false;
  }

  // TODO: Check for success of commit, currently (2019-01-11) not possible.
  transaction_context->commit();

  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());
  return true;
}

void MvccDeleteManager::_delete_physically(const std::string &tableName, const opossum::ChunkID chunkID) {
  // Assert: Logical delete must have happened to this point.

  // TODO later
}


}  // namespace opossum