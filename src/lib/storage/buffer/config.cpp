#include "config.hpp"
#include <fstream>
#include "hyrise.hpp"

namespace hyrise {

BufferManagerConfig::BufferManagerConfig(const Topology& topology) {
  // set_default_pool_sizes(topology);
  // set_default_ssd_path();
}

BufferManagerConfig::BufferManagerConfig() : BufferManagerConfig{Hyrise::get().topology} {};

BufferManagerConfig BufferManagerConfig::from_env_or_default(const Topology& topology) {
  nlohmann::json json;

  if (const auto json_path = std::getenv("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH")) {
    auto json_file = std::ifstream(json_path);
    if (!json_file.is_open()) {
      Fail("Failed to open HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH file");
    }

    try {
      json_file >> json;
    } catch (const std::exception& e) {
      Fail("Failed to parse HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH file: " + std::string(e.what()));
    }
  } else {
    std::cout << "HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH not set, using default values" << std::endl;
  }

  auto config = BufferManagerConfig{};
  config.memory_node = static_cast<NodeID>(json.value("memory_node", static_cast<int64_t>(config.memory_node)));
  config.cpu_node = static_cast<NodeID>(json.value("cpu_node", static_cast<int64_t>(config.cpu_node)));
  config.set_default_pool_sizes(topology);

  config.dram_buffer_pool_size = json.value("dram_buffer_pool_size", config.dram_buffer_pool_size);
  config.numa_buffer_pool_size = json.value("numa_buffer_pool_size", config.numa_buffer_pool_size);

  if (std::filesystem::is_block_file(json.value("ssd_path", config.ssd_path)) ||
      std::filesystem::is_directory(json.value("ssd_path", config.ssd_path))) {
    config.ssd_path = json.value("ssd_path", config.ssd_path);
  } else {
    Fail("ssd_path is neither a block device nor a directory");
  }
  auto migration_policy_json = json.value("migration_policy", nlohmann::json{});
  config.migration_policy =
      MigrationPolicy{migration_policy_json.value("dram_read_ratio", config.migration_policy.get_dram_read_ratio()),
                      migration_policy_json.value("dram_write_ratio", config.migration_policy.get_dram_write_ratio()),
                      migration_policy_json.value("numa_read_ratio", config.migration_policy.get_numa_read_ratio()),
                      migration_policy_json.value("numa_write_ratio", config.migration_policy.get_numa_write_ratio())};

  return config;
}

nlohmann::json BufferManagerConfig::to_json() const {
  auto json = nlohmann::json{};
  json["dram_buffer_pool_size"] = dram_buffer_pool_size;
  json["numa_buffer_pool_size"] = numa_buffer_pool_size;
  json["ssd_path"] = ssd_path;
  json["migration_policy"]["dram_read_ratio"] = migration_policy.get_dram_read_ratio();
  json["migration_policy"]["dram_write_ratio"] = migration_policy.get_dram_write_ratio();
  json["migration_policy"]["numa_read_ratio"] = migration_policy.get_numa_read_ratio();
  json["migration_policy"]["numa_write_ratio"] = migration_policy.get_numa_write_ratio();
  json["memory_node"] = static_cast<int64_t>(memory_node);
  json["cpu_node"] = static_cast<int64_t>(cpu_node);
  return json;
}

void BufferManagerConfig::set_default_pool_sizes(const Topology& topology) {
  // TODO: Use free bytes
  dram_buffer_pool_size = topology.total_bytes(cpu_node) * 8 / 10;
  numa_buffer_pool_size = topology.total_bytes(memory_node) * 8 / 10;
}

}  // namespace hyrise