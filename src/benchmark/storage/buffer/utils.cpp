#include "storage/buffer/utils.hpp"

namespace hyrise {

void add_buffer_manager_counters(benchmark::State& state, BufferManager& buffer_manager) {
  const auto metrics = buffer_manager.metrics();
  // TODO
  // state.counters["total_allocated_bytes"] =
  //     benchmark::Counter(static_cast<double>(metrics.total_allocated_bytes), benchmark::Counter::Flags::kDefaults,
  //                        benchmark::Counter::OneK::kIs1024);
  // state.counters["total_unused_bytes"] =
  //     benchmark::Counter(static_cast<double>(metrics.total_unused_bytes), benchmark::Counter::Flags::kDefaults,
  //                        benchmark::Counter::OneK::kIs1024);
  // state.counters["page_table_hits"] = static_cast<double>(metrics.page_table_hits);
  // state.counters["page_table_misses"] = static_cast<double>(metrics.page_table_misses);
  // state.counters["total_bytes_read"] =
  //     benchmark::Counter(static_cast<double>(metrics.total_bytes_read_from_ssd), benchmark::Counter::Flags::kDefaults,
  //                        benchmark::Counter::OneK::kIs1024);
  // state.counters["total_bytes_written"] =
  //     benchmark::Counter(static_cast<double>(metrics.total_bytes_written_to_ssd), benchmark::Counter::Flags::kDefaults,
  //                        benchmark::Counter::OneK::kIs1024);

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

MetricsSampler::MetricsSampler(const std::string& name, const std::filesystem::path& output_path,
                               BufferManager* buffer_manager)
    : _name(name), _buffer_manager(buffer_manager), _output_path(output_path) {
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
  auto metrics_json = nlohmann::json::array();
  auto timestamp = Duration::zero();
  for (auto& metric : _metrics) {
    auto metric_json = nlohmann::json::object();
    to_json(metric_json, timestamp, metric);
    metrics_json.push_back(metric_json);
    timestamp += _interval;
  }
  auto context = nlohmann::json::object();
  context["interval"] = _interval.count();
  context["name"] = _name;
  context["bytes_dram_buffer_pools"] = _buffer_manager->get_config().dram_buffer_pool_size;
  context["bytes_numa_buffer_pools"] = _buffer_manager->get_config().numa_buffer_pool_size;
  context["mode"] = magic_enum::enum_name(_buffer_manager->get_config().mode);
  context["eviction_worker_enabled"] = _buffer_manager->get_config().enable_eviction_purge_worker;
  context["ssd_path"] = _buffer_manager->get_config().ssd_path;

  auto migration_policy = nlohmann::json::object();
  migration_policy["dram_read_ratio"] = _buffer_manager->get_config().migration_policy.get_dram_read_ratio();
  migration_policy["dram_write_ratio"] = _buffer_manager->get_config().migration_policy.get_dram_write_ratio();
  migration_policy["numa_read_ratio"] = _buffer_manager->get_config().migration_policy.get_numa_read_ratio();
  migration_policy["numa_write_ratio"] = _buffer_manager->get_config().migration_policy.get_numa_write_ratio();

  context["migration_policy"] = migration_policy;

  auto report = nlohmann::json{{"context", context}, {"metrics", metrics_json}};
  std::ofstream{_output_path} << std::setw(2) << report << std::endl;
}

void MetricsSampler::to_json(nlohmann::json& json, Duration timestamp, const BufferManager::Metrics& metrics) {
  json = {
      {"timestamp", timestamp.count()},
      {"current_bytes_used", metrics.current_bytes_used_dram},
      {"current_pins", metrics.current_pins_dram},
      {"total_allocated_bytes", metrics.total_allocated_bytes_dram},

      {"num_allocs", metrics.num_allocs},
      {"num_deallocs", metrics.num_deallocs},

      {"total_hits_dram", metrics.total_hits_dram},
      {"total_hits_numa", metrics.total_hits_numa},
      {"total_misses_dram", metrics.total_misses_dram},
      {"total_misses_numa", metrics.total_misses_numa},

      {"total_bytes_copied_from_ssd_to_dram", metrics.total_bytes_copied_from_ssd_to_dram},
      {"total_bytes_copied_from_ssd_to_numa", metrics.total_bytes_copied_from_ssd_to_numa},
      {"total_bytes_copied_from_numa_to_dram", metrics.total_bytes_copied_from_numa_to_dram},
      {"total_bytes_copied_from_dram_to_numa", metrics.total_bytes_copied_from_dram_to_numa},
      {"total_bytes_copied_from_dram_to_ssd", metrics.total_bytes_copied_from_dram_to_ssd},
      {"total_bytes_copied_from_numa_to_ssd", metrics.total_bytes_copied_from_numa_to_ssd},
      {"total_bytes_copied_to_ssd", metrics.total_bytes_copied_to_ssd},
      {"total_bytes_copied_from_ssd", metrics.total_bytes_copied_from_ssd},

      {"num_madvice_free_calls_numa", metrics.num_madvice_free_calls_numa},
      {"num_madvice_free_calls_dram", metrics.num_madvice_free_calls_dram},

  };
}

void MetricsSampler::sample() {
  _metrics.push_back(_buffer_manager->metrics());
}

}  // namespace hyrise