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

struct SegmentInfo {
  SegmentInfo(const std::string& table_name, const ChunkID chunk_id, const ColumnID column_id,
              const size_t memory_usage, SegmentAccessCounter<uint64_t> access_counter)
    : table_name{table_name}, chunk_id{chunk_id}, column_id{column_id}, memory_usage{memory_usage},
      access_counter{std::move(access_counter)} {}

  std::string table_name;
  ChunkID chunk_id;
  ColumnID column_id;
  size_t memory_usage;
  SegmentAccessCounter<uint64_t> access_counter;
};
}

using namespace anticaching;

class AntiCachingPlugin : public AbstractPlugin {
  friend class AntiCachingPluginTest;

 public:
  const std::string description() const;

  void start();

  void stop();

  void export_access_statistics(const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  void reset_access_statistics();

  size_t memory_budget = 1000ul * 1024ul * 10204ul;

 private:
  using SegmentIDAccessCounterPair = std::pair<const SegmentID, const SegmentAccessCounter<uint64_t>>;
  using TimestampSegmentInfoPair = std::pair<const std::chrono::time_point<std::chrono::steady_clock>, std::vector<SegmentInfo>>;

  static std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> _fetch_segments();
  std::vector<SegmentInfo> _fetch_current_statistics();

  void _evaluate_statistics();
  static float _compute_value(const SegmentInfo& segment_info);
  void _evict_segments();

  std::vector<TimestampSegmentInfoPair> _access_statistics;
  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;
  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(10'000);
};

}  // namespace opossum

