#include <operators/get_table.hpp>
#include <operators/validate.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/insert.hpp>
#include <operators/delete.hpp>
#include "mvcc_manager.hpp"
#include "storage/storage_manager.hpp"
#include "concurrency/transaction_manager.hpp"


void opossum::MvccManager::testDeleteChunkLogically(const std::string &tableName, const opossum::ChunkID chunkID) {
  deleteChunkLogically(tableName, chunkID);
}

void opossum::MvccManager::deleteChunkLogically(const std::string &tableName, const opossum::ChunkID chunkID) {

  auto& sm = StorageManager::get();
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // Put relevant chunk into a temporary table
  const auto table = sm.get_table(tableName);
  const auto table_extract = std::make_shared<Table>(table->column_definitions(), TableType::Data,
                                                          table->max_chunk_size(), table->has_mvcc());
  table_extract->append_chunk(table->get_chunk(chunkID));
  auto table_wrapper = std::make_shared<TableWrapper>(table_extract);
  table_wrapper->execute();

  // Filter valid records
  auto validate_table = std::make_shared<Validate>(table_wrapper);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Re-insert valid records into table (most recent chunk)
  auto insert_op = std::make_shared<Insert>(tableName, validate_table);
  insert_op->set_transaction_context(transaction_context);
  insert_op->execute();

  // Remove records (old chunk) from table
  auto delete_op = std::make_shared<Delete>(tableName, validate_table);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();
}

void opossum::MvccManager::deleteChunkPhysically(const std::string &tableName, const opossum::ChunkID chunkID) {
  // TODO later
}
