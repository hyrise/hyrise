#pragma once

#include <queue>
#include <mutex>
#include <thread>
#include <utils/pausable_loop_thread.hpp>
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"
#include "gtest/gtest_prod.h"

namespace opossum {

class MvccDeletePlugin : public AbstractPlugin, public Singleton<MvccDeletePlugin> {
 friend class MvccDeleteTest;

 public:
  MvccDeletePlugin();

  const std::string description() const final;

  void start() final;

  void stop() final;

 private:
    struct ChunkSpecifier {
        std::string table_name;
        ChunkID chunk_id;
        ChunkSpecifier(std::string table_name, ChunkID chunk_id) : table_name(std::move(table_name)), chunk_id(chunk_id) { }
    };

    void _logical_delete_loop();
    void _physical_delete_loop();

    void _delete_chunk(const std::string &table_name, ChunkID chunk_id);
    static bool _delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
    static bool _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);
    double _invalidated_rows_amount(std::shared_ptr<Chunk> chunk) const;

    static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);


    std::mutex _mutex;
    std::unique_ptr<PausableLoopThread> _loop_thread_logical_delete, _loop_thread_physical_delete;

    StorageManager& _sm;
    double _delete_threshold_share_invalidated_rows;
    std::chrono::milliseconds _idle_delay_logical_delete;
    std::chrono::milliseconds _idle_delay_physical_delete;

    std::queue<ChunkSpecifier> _physical_delete_queue;
};

}  // namespace opossum
