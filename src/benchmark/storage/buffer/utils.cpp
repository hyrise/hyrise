#include "utils.hpp"

namespace hyrise {

void add_buffer_manager_counters(benchmark::State& state, BufferManager& buffer_manager) {
  const auto metrics = buffer_manager.metrics();
  state.counters["peak_bytes_used"] = static_cast<double>(metrics.max_bytes_used);
  state.counters["total_allocated_bytes"] = static_cast<double>(metrics.total_allocated_bytes);
  state.counters["total_unused_bytes"] = static_cast<double>(metrics.total_unused_bytes); 
  state.counters["page_table_hits"] = static_cast<double>(metrics.page_table_hits);
  state.counters["page_table_misses"] = static_cast<double>(metrics.page_table_misses);
  state.counters["total_bytes_read"] = static_cast<double>(metrics.total_bytes_read);
  state.counters["total_bytes_written"] = static_cast<double>(metrics.total_bytes_written);
  // TODO: read and write rate, 
}

std::filesystem::path ssd_region_path() {
  if (const char* path = std::getenv("HYRISE_BUFFER_MANAGER_PATH")) {
    return path;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_PATH not found in environment for benchmarks");
  }
}

}