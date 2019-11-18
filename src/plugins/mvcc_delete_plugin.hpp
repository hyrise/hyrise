#pragma once

#include <algorithm>
#include <mutex>
#include <numeric>
#include <queue>
#include <thread>

#include "gtest/gtest_prod.h"
#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "utils/singleton.hpp"

namespace opossum {

/*
 * One disadvantage of insert-only databases like Hyrise is the accumulation of invalidated
 * rows, which have to be removed from the final result for every transaction.
 * This plugin deletes chunks with large numbers of invalidated rows and reinserts them at
 * the end of the table. These rows are either visible at their original position (for old
 * transactions) or their new position (for new transactions). Thus, it keeps the
 * execution time per transaction low and the database maintains its original performance.
 * The plugin is split into two main functions. The logical delete is responsible for
 * recognizing chunks with high numbers of invalidated rows and fully invalidates them.
 * The physical delete checks if chunks are not visible anymore for other transactions and
 * removes the chunk from the table completely.
 */
class MvccDeletePlugin : public AbstractPlugin {
  friend class MvccDeletePluginTest;
  friend class MvccDeletePluginSystemTest;

 public:
  const std::string description() const final;

  void start() final;

  void stop() final;

  /**
   * DELETE_THRESHOLD_PERCENTAGE_INVALIDATED_ROWS: the percentage of invalidated rows
   * in chunk to be deleted logically by the plugin.
   * DELETE_THRESHOLD_LAST_COMMIT: the number of commits that must have passed since
   * the candidate chunk was last modified
   * IDLE_DELAY_LOGICAL_DELETE: sleep after execution of logical delete
   * IDLE_DELAY_PHYSICAL_DELETE: sleep after execution of physical delete
   */
  constexpr static double DELETE_THRESHOLD_PERCENTAGE_INVALIDATED_ROWS = 0.6;
  constexpr static CommitID DELETE_THRESHOLD_LAST_COMMIT = CommitID{100};
  constexpr static std::chrono::milliseconds IDLE_DELAY_LOGICAL_DELETE = std::chrono::milliseconds(1000);
  constexpr static std::chrono::milliseconds IDLE_DELAY_PHYSICAL_DELETE = std::chrono::milliseconds(1000);

 private:
  using TableAndChunkID = std::pair<const std::shared_ptr<Table>&, ChunkID>;

  void _logical_delete_loop();
  void _physical_delete_loop();

  static bool _try_logical_delete(const std::string& table_name, ChunkID chunk_id,
                                  std::shared_ptr<TransactionContext> transaction_context);
  static void _delete_chunk_physically(const std::shared_ptr<Table>& table, ChunkID chunk_id);

  std::unique_ptr<PausableLoopThread> _loop_thread_logical_delete, _loop_thread_physical_delete;

  std::mutex _mutex_physical_delete_queue;
  std::queue<TableAndChunkID> _physical_delete_queue;
};

}  // namespace opossum
