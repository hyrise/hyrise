#include <memory>
#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

/**
 * BufferManagerBenchmarkMemoryManager is a utility class that registers a buffer manager as
 * the benchmark::MemoryManager for a Google benchmark. This is useful for tracking common memory related metrics.
*/
class BufferManagerBenchmarkMemoryManager : public benchmark::MemoryManager {
 public:
  void Start() override {
    _buffer_manager->metrics() = BufferManager::Metrics{};
  }

  void Stop(Result* result) override {
    result->num_allocs = _buffer_manager->metrics().num_allocs;
    result->max_bytes_used = _buffer_manager->metrics().max_bytes_used;
    result->total_allocated_bytes = _buffer_manager->metrics().total_allocated_bytes;
    // TODO: result->net_heap_growth
    // The net changes in memory, in bytes, between Start and Stop.
    // ie., total_allocated_bytes - total_deallocated_bytes.
    // Init'ed to TombstoneValue if metric not available.
  }

  void Stop(Result& result) override {
    result.num_allocs = _buffer_manager->metrics().num_allocs;
    result.max_bytes_used = _buffer_manager->metrics().max_bytes_used;
    result.total_allocated_bytes = _buffer_manager->metrics().total_allocated_bytes;
  }

  BufferManagerBenchmarkMemoryManager(BufferManager* buffer_manager) : _buffer_manager(buffer_manager) {}

  static std::unique_ptr<BufferManagerBenchmarkMemoryManager> create_and_register(
      BufferManager* buffer_manager = &Hyrise::get().buffer_manager) {
    auto manager = std::make_unique<BufferManagerBenchmarkMemoryManager>(buffer_manager);
    RegisterMemoryManager(manager.get());
    return manager;
  }

 private:
  BufferManager* _buffer_manager;
};

static std::filesystem::path ssd_region_path() {
  if(const char* path = std::getenv("HYRISE_BUFFER_MANAGER_PATH")) {
    return path;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_PATH not found in environment for benchmarks");
  }
}

}  // namespace hyrise