#pragma once

#include <queue>
#include <concurrency/mvcc_delete_manager.hpp>
#include "MvccDelete"

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class MvccDeletePlugin : public AbstractPlugin, public Singleton<MvccDeletePlugin> {
 public:
  MvccDeletePlugin() : sm(StorageManager::get()) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& sm;


protected:

    void _clean_up_chunk(const std::string &table_name, ChunkID chunk_id);
    void _process_physical_delete_queue();

    struct ChunkSpecifier {
        std::string table_name;
        ChunkID chunk_id;
    };

    std::queue<ChunkSpecifier> _physical_delete_queue;
};

}  // namespace opossum
