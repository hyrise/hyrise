#pragma once

#include <chrono>
#include <memory>
#include <utility>
#include <string>

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class AntiCachingPlugin : public AbstractPlugin {
 public:
  const std::string description() const;

  void start();

  void stop();

  void export_access_statistics(const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  void reset_access_statistics();



 private:
  using TableNameChunkIDsPair = std::pair<const std::string, std::vector<SegmentAccessStatisticsTools::ChunkIDColumnIDsPair>>;
  using TimestampTableNamesPair = std::pair<const std::chrono::time_point<std::chrono::steady_clock>, std::vector<TableNameChunkIDsPair>>;

  void _evaluate_statistics();
  std::vector<TableNameChunkIDsPair> _fetch_current_statistcs();



  std::vector<TimestampTableNamesPair> _access_statistics;
  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;
  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(1000);
};

}  // namespace opossum

