#pragma once

#include <functional>
#include <unordered_map>

#include "storage/storage_manager.hpp"

namespace opossum {

class MetaTableManager : public Singleton<MetaTableManager> {
  friend class Singleton<MetaTableManager>;

 public:
  // We need to pass the StorageManager so that update_all can be called while the StorageManager is still being
  // constructed
  void update_all(StorageManager& storage_manager);

  void update(StorageManager& storage_manager, const std::string& name);

  void update_chunk_table(StorageManager& storage_manager);
  void update_segment_table(StorageManager& storage_manager);

 protected:
  MetaTableManager();

  std::unordered_map<std::string, std::function<void(StorageManager&)>> _methods;
};

}  // namespace opossum
