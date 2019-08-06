#pragma once

#include <functional>
#include <unordered_map>

#include "storage/storage_manager.hpp"

namespace opossum {

class MetaTableManager : public Singleton<MetaTableManager> {  // TODO Move to cpp
 friend class Singleton<MetaTableManager>;

 public:
  void update_all(StorageManager& storage_manager) {
    // We need to pass the StorageManager so that update_all can be called while the StorageManager is still being constructed
    for (const auto& [table_name, method] : _methods) {
      method(storage_manager);
    }
  }

  void update(StorageManager& storage_manager, const std::string& name) {
    _methods.at(name)(storage_manager);
  }

  void update_chunk_table(StorageManager& storage_manager) {
    if (storage_manager.has_table("meta_chunks")) storage_manager.drop_table("meta_chunks");

    const auto columns = TableColumnDefinitions{{"table", DataType::String}, {"chunk_id", DataType::Int}, {"rows", DataType::Long}, {"invalid_rows", DataType::Long}, {"cleanup_commit_id", DataType::Long}};
    auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

    for (const auto& [table_name, table] : storage_manager.tables()) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto cleanup_commit_id = chunk->get_cleanup_commit_id() ? static_cast<int64_t>(*chunk->get_cleanup_commit_id) : int64_t{0};
        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int64_t>(chunk->size()), static_cast<int64_t>(chunk->invalid_row_count()), cleanup_commit_id});
      }
    }

    storage_manager.add_table("meta_chunks", output_table);
  }

 protected:
  MetaTableManager() {
    _methods["chunks"] = std::bind(&MetaTableManager::update_chunk_table, this, std::placeholders::_1);
  }

  std::unordered_map<std::string, std::function<void(StorageManager&)>> _methods;
};

}  // namespace opossum
