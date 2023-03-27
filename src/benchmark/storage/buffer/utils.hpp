#pragma once

#include <algorithm>
#include <memory>
#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "nlohmann/json.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

// TODO: Add more context about machine, SSD and the Page konfigurations
// USe ClobberMemory etc

/**
 * BufferManagerBenchmarkMemoryManager is a utility class that registers a buffer manager as
 * the benchmark::MemoryManager for a Google benchmark. This is useful for tracking common memory related metrics.
*/
class BufferManagerBenchmarkMemoryManager : public benchmark::MemoryManager {
 public:
  void Start() override {
    _metrics_snapshot = _buffer_manager->metrics();
  }

  void Stop(Result& result) override {
    const auto metrics = _buffer_manager->metrics();
    result.num_allocs = metrics.num_allocs - _metrics_snapshot.num_allocs;
    result.max_bytes_used = std::max(metrics.max_bytes_used, _metrics_snapshot.num_allocs);
    result.total_allocated_bytes = metrics.total_allocated_bytes - _metrics_snapshot.total_allocated_bytes;
    // TODO: result->net_heap_growth
    // The net changes in memory, in bytes, between Start and Stop.
    // ie., total_allocated_bytes - total_deallocated_bytes.
    // Init'ed to TombstoneValue if metric not available.
  }

  BufferManagerBenchmarkMemoryManager(BufferManager* buffer_manager) : _buffer_manager(buffer_manager) {}

  static std::unique_ptr<BufferManagerBenchmarkMemoryManager> create_and_register(
      BufferManager* buffer_manager = &Hyrise::get().buffer_manager) {
    auto manager = std::make_unique<BufferManagerBenchmarkMemoryManager>(buffer_manager);
    RegisterMemoryManager(manager.get());
    return manager;
  }

 private:
  BufferManager::Metrics _metrics_snapshot;
  BufferManager* _buffer_manager;
};

/// TODO: Move into impl file
class MetricsSampler {
 public:
  MetricsSampler(const std::filesystem::path& output_path, BufferManager* buffer_manager)
      : _buffer_manager(buffer_manager), _output_path(output_path) {
    start();
  }

  ~MetricsSampler() {
    stop();
  }

  void start() {
    if (_sample_thread) {
      Fail("MetricsSampler is already running");
    }
    _sample_thread = std::make_unique<PausableLoopThread>(_interval, [this](size_t) { sample(); });
  }

  void stop() {
    export_metrics();
    _sample_thread = nullptr;
    _metrics.clear();
  }

 private:
  void export_metrics() {
    // TODO: Add context
    auto metrics_json = nlohmann::json::array();
    for (auto& metric : _metrics) {
      auto metric_json = nlohmann::json::object();
      to_json(metric_json, metric);
      metrics_json.push_back(metric_json);
    }
    nlohmann::json report{{"metrics", metrics_json}};

    std::ofstream{_output_path} << std::setw(2) << report << std::endl;
  }

  void to_json(nlohmann::json& json, const BufferManager::Metrics& metrics) {
    json = {
        {"max_bytes_used", metrics.max_bytes_used},
        {"total_allocated_bytes", metrics.total_allocated_bytes},
        {"num_allocs", metrics.num_allocs},
        {"num_deallocs", metrics.num_deallocs},
    };
  }

  void sample() {
    _metrics.push_back(_buffer_manager->metrics());
  }

  std::chrono::milliseconds _interval = std::chrono::milliseconds(100);
  std::unique_ptr<PausableLoopThread> _sample_thread;
  BufferManager* _buffer_manager;
  std::vector<BufferManager::Metrics> _metrics;
  const std::filesystem::path _output_path;
};

/**
 * Add specific counters for the buffer manager. 
 * TODO: Optionally supply an existing metric struct for save the difference. 
*/
void add_buffer_manager_counters(benchmark::State& state, BufferManager& buffer_manager);

std::filesystem::path ssd_region_scratch_path();
std::filesystem::path ssd_region_block_path();

}  // namespace hyrise