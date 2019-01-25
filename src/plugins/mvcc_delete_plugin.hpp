#pragma once

#include <queue>
#include <concurrency/mvcc_delete_manager.hpp>

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

 private:
  bool run_delete(const std::string& table_name, const ChunkID chunk_id) const;
  std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, const ChunkID chunk_id) const;

  StorageManager& sm;
  const double DELETE_THRESHOLD = 0.9;

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
