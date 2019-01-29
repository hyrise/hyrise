#pragma once

#include <queue>
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"
#include "gtest/gtest_prod.h"

namespace opossum {

class MvccDeletePlugin : public AbstractPlugin, public Singleton<MvccDeletePlugin> {

  FRIEND_TEST(MvccDeleteTest, LogicalDelete);
  FRIEND_TEST(MvccDeleteTest, PhysicalDelete);
  FRIEND_TEST(MvccDeleteTest, PhysicalDelete_NegativePrecondition_cleanup_commit_id);

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

    void _begin_cleanup();
    void _finish_cleanup();

    void _delete_chunk(const std::string &table_name, ChunkID chunk_id);
    static bool _delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
    static bool _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);

    static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);


    std::mutex _mutex_queue;
    StorageManager& _sm;
    double _delete_threshold_share_invalidated_rows;
    std::chrono::milliseconds _check_interval_physical_delete;
    std::queue<ChunkSpecifier> _physical_delete_queue;
};

}  // namespace opossum
