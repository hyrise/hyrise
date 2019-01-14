#include <operators/get_table.hpp>
#include <operators/validate.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/update.hpp>
#include <storage/reference_segment.hpp>
#include "mvcc_delete_manager.hpp"
#include "storage/storage_manager.hpp"
#include "concurrency/transaction_manager.hpp"


namespace opossum {

void MvccDeleteManager::run_logical_delete(const std::string &tableName, ChunkID chunkID) {
  _delete_logically(tableName, chunkID);
}

bool MvccDeleteManager::_delete_logically(const std::string &tableName, const ChunkID chunkID) {
  auto& sm = StorageManager::get();
  const auto table = sm.get_table(tableName);
  std::cout << "table with chunk - table address: " << table.get() << std::endl;
  auto chunk = table->get_chunk(chunkID);

  // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
  DebugAssert(chunkID < (table->chunk_count() - 1), "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

  // Create temporary table from chunk
  const auto table_extract = std::make_shared<Table>(table->column_definitions(), TableType::Data,
                                                          table->max_chunk_size(), table->has_mvcc());
  std::cout << "table_extract->append_chunk" << std::endl;
  table_extract->append_chunk(chunk);
  std::cout << "table_extract->append_chunk done!" << std::endl;
  auto table_wrapper = std::make_shared<TableWrapper>(table_extract);
  table_wrapper->execute();
  std::cout << "table_wrapper done" << std::endl;
  std::cout << "table_wrapper - table address: " << table_wrapper->get_output().get() << std::endl;

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(table_wrapper);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();
  std::cout << "validate_table done" << std::endl;

  std::shared_ptr<const Table> validate_table_output = validate_table->get_output();
  std::shared_ptr<const ReferenceSegment> first_ref_segment = std::dynamic_pointer_cast<ReferenceSegment>(validate_table_output->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  if(first_ref_segment) {
    std::cout << "validate_table, first_ref_segment, referenced_table - table address: " << first_ref_segment->referenced_table().get() << std::endl;
  }
  else {
    std::cout << "first_ref_segment == null" << std::endl;
  }
  // Use UPDATE operator to DELETE and RE-INSERT valid records in chunk
    // Pass validate_table as input twice since data will not be changed.
  auto update_table = std::make_shared<Update>(tableName, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();
  std::cout << "update_table done" << std::endl;

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