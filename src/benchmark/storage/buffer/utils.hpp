#pragma once

#include <algorithm>
#include <fstream>
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
    // const auto metrics = _buffer_manager->metrics();
    // result.num_allocs = metrics.num_allocs - _metrics_snapshot.num_allocs;
    // result.total_allocated_bytes = metrics.total_allocated_bytes - _metrics_snapshot.total_allocated_bytes;
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

class MetricsSampler {
 public:
  MetricsSampler(const std::filesystem::path& output_path, BufferManager* buffer_manager);
  ~MetricsSampler();

  void start();
  void stop();

  void export_metrics();

 private:
  using Duration = std::chrono::duration<float, std::milli>;
  void to_json(nlohmann::json& json, Duration timestamp, const BufferManager::Metrics& metrics);
  void sample();

  std::chrono::milliseconds _interval = std::chrono::milliseconds(10);
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