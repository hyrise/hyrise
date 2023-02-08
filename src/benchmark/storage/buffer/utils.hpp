#include <memory>
#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {
class BufferManagerBenchmarkMemoryManager : public benchmark::MemoryManager {
 public:
  void Start() override {}

  void Stop(Result* result) override {
    result->num_allocs = _buffer_manager->metrics().allocations_in_bytes.size();
    result->max_bytes_used = 2423424;
    result->total_allocated_bytes = 2343;
    result->net_heap_growth =234;
  }

  void Stop(Result& result) override {
    // TODO: Add more stats here, and maybe adapt if buffer is not reset
    result.num_allocs = _buffer_manager->metrics().allocations_in_bytes.size();
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

}  // namespace hyrise