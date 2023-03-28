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
      benchmark::Counter(static_cast<double>(metrics.total_bytes_read_from_ssd), benchmark::Counter::Flags::kDefaults,
                         benchmark::Counter::OneK::kIs1024);
  state.counters["total_bytes_written"] =
      benchmark::Counter(static_cast<double>(metrics.total_bytes_written_to_ssd), benchmark::Counter::Flags::kDefaults,
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

MetricsSampler::MetricsSampler(const std::filesystem::path& output_path, BufferManager* buffer_manager)
    : _buffer_manager(buffer_manager), _output_path(output_path) {
  start();
}

MetricsSampler::~MetricsSampler() {
  stop();
}

void MetricsSampler::start() {
  if (_sample_thread) {
    Fail("MetricsSampler is already running");
  }
  _sample_thread = std::make_unique<PausableLoopThread>(_interval, [this](size_t) { sample(); });
}

void MetricsSampler::stop() {
  export_metrics();
  _sample_thread = nullptr;
  _metrics.clear();
}

void MetricsSampler::export_metrics() {
  // TODO: Add context
  auto metrics_json = nlohmann::json::array();
  for (auto& metric : _metrics) {
    auto metric_json = nlohmann::json::object();
    to_json(metric_json, metric);
    metrics_json.push_back(metric_json);
  }
  auto context = nlohmann::json::object();
  context["interval"] = _interval.count();
  auto report = nlohmann::json{{"context", context}, {"metrics", metrics_json}};
  std::ofstream{_output_path} << std::setw(2) << report << std::endl;
}

void MetricsSampler::to_json(nlohmann::json& json, const BufferManager::Metrics& metrics) {
  json = {
      {"current_bytes_used", metrics.current_bytes_used},
      {"max_bytes_used", metrics.max_bytes_used},
      {"total_allocated_bytes", metrics.total_allocated_bytes},
      {"num_allocs", metrics.num_allocs},
      {"num_deallocs", metrics.num_deallocs},
      {"total_bytes_written_to_ssd", metrics.total_bytes_written_to_ssd},
      {"total_bytes_read_from_ssd", metrics.total_bytes_read_from_ssd},
      {"page_table_hits", metrics.page_table_hits},
      {"page_table_misses", metrics.page_table_misses},
  };
}

void MetricsSampler::sample() {
  _metrics.push_back(_buffer_manager->metrics());
}

}  // namespace hyrise