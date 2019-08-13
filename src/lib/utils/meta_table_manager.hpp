#pragma once

#include <functional>
#include <unordered_map>

#include "storage/storage_manager.hpp"

namespace opossum {

class MetaTableManager : public Singleton<MetaTableManager> {
  friend class Singleton<MetaTableManager>;

 public:
  static constexpr auto META_PREFIX = "meta_";

  // We need to pass the StorageManager so that update_all can be called while the StorageManager is still being
  // constructed
  void update_all(StorageManager& storage_manager);

  void update(StorageManager& storage_manager, const std::string& table_name);

  std::shared_ptr<Table> generate_tables_table(const StorageManager&);
  std::shared_ptr<Table> generate_columns_table(const StorageManager&);
  std::shared_ptr<Table> generate_chunks_table(const StorageManager&);
  std::shared_ptr<Table> generate_segments_table(const StorageManager&);

 protected:
  MetaTableManager();

  std::unordered_map<std::string, std::function<std::shared_ptr<Table>(const StorageManager&)>> _methods;
};

}  // namespace opossum
