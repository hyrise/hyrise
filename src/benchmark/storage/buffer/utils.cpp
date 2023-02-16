#include "utils.hpp"

namespace hyrise {

void add_buffer_manager_counters(benchmark::State& state, BufferManager& buffer_manager) {
  const auto metrics = buffer_manager.metrics();
  state.counters["peak_bytes_used"] =
      benchmark::Counter(static_cast<double>(metrics.max_bytes_used), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);
  state.counters["total_allocated_bytes"] =
      benchmark::Counter(static_cast<double>(metrics.total_allocated_bytes), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);
  state.counters["total_unused_bytes"] =
      benchmark::Counter(static_cast<double>(metrics.total_unused_bytes), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);
  state.counters["page_table_hits"] = static_cast<double>(metrics.page_table_hits);
  state.counters["page_table_misses"] = static_cast<double>(metrics.page_table_misses);
  state.counters["total_bytes_read"] =
      benchmark::Counter(static_cast<double>(metrics.total_bytes_read), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);
  state.counters["total_bytes_written"] =
      benchmark::Counter(static_cast<double>(metrics.total_bytes_written), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);

  // TODO: read and write rate,
}

std::filesystem::path ssd_region_scratch_path() {
  if (const char* path = std::getenv("HYRISE_BUFFER_MANAGER_SCRATCH_PATH")) {
    return path;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_SCRATCH_PATH not found in environment for benchmarks");
  }
}

std::filesystem::path ssd_region_block_path() {
  if (const char* path = std::getenv("HYRISE_BUFFER_MANAGER_BLOCK_PATH")) {
    return path;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_BLOCK_PATH not found in environment for benchmarks");
  }
}

}  // namespace hyrise