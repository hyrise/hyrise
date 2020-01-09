#pragma once

#include <chrono>
#include <memory>
#include <utility>
#include <string>

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "storage/base_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

namespace anticaching {
  struct SegmentID {
    SegmentID(const std::string& table_name, const ChunkID chunk_id, const ColumnID column_id)
      : table_name{table_name}, chunk_id{chunk_id}, column_id{column_id} {}
    std::string table_name;
    ChunkID chunk_id;
    ColumnID column_id;
  };
}

using namespace anticaching;
class AntiCachingPlugin : public AbstractPlugin {
 public:
  const std::string description() const;

  void start();

  void stop();

  void export_access_statistics(const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  void reset_access_statistics();



 private:
  using SegmentIDAccessCounterPair = std::pair<const SegmentID, const SegmentAccessCounter<uint64_t>>;
  using TimestampSegmentIDsPair = std::pair<const std::chrono::time_point<std::chrono::steady_clock>, std::vector<SegmentIDAccessCounterPair>>;

  void _evaluate_statistics();
  static std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> _fetch_segments();
  std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> _segments;
  std::vector<SegmentIDAccessCounterPair> _fetch_current_statistics();

  std::vector<TimestampSegmentIDsPair> _access_statistics;
  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;
  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(1000);
};

}  // namespace opossum

