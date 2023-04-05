#pragma once
#include <cstdlib>
#include <filesystem>
#include "storage/buffer/migration_policy.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

uint64_t get_dram_capacity_from_env() {
  if (const auto volatile_capacity = std::getenv("HYRISE_BUFFER_MANAGER_DRAM_CAPACITY")) {
    return boost::lexical_cast<uint64_t>(volatile_capacity);
  } else {
    Fail("HYRISE_BUFFER_MANAGER_DRAM_CAPACITY not found in environment");
  }
}

uint64_t get_numa_capacity_from_env() {
  if (const auto volatile_capacity = std::getenv("HYRISE_BUFFER_MANAGER_NUMA_CAPACITY")) {
    return boost::lexical_cast<uint64_t>(volatile_capacity);
  } else {
    Fail("HYRISE_BUFFER_MANAGER_NUMA_CAPACITY not found in environment");
  }
}

std::filesystem::path get_ssd_region_file_from_env() {
  if (const auto ssd_region_path = std::getenv("HYRISE_BUFFER_MANAGER_SSD_PATH")) {
    if (std::filesystem::is_block_file(ssd_region_path)) {
      return ssd_region_path;
    }
    const auto path = std::filesystem::path(ssd_region_path);
    const auto now = std::chrono::system_clock::now();
    const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    const auto db_file = std::filesystem::path{"hyrise-buffer-pool-" + std::to_string(timestamp) + ".bin"};
    return path / db_file;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_SSD_PATH not found in environment");
  }
}

MigrationPolicy get_migration_policy_from_env() {
  if (const auto migration_policy_string = std::getenv("HYRISE_BUFFER_MANAGER_MIGRATION_POLICY")) {
    if (std::string(migration_policy_string) == "eager") {
      return EagerMigrationPolicy{};
    } else if (std::string(migration_policy_string) == "lazy") {
      return LazyMigrationPolicy{};
    } else {
      Fail("HYRISE_BUFFER_MANAGER_MIGRATION_POLICY needs to be either 'eager' or 'lazy'");
    }
  } else {
    return EagerMigrationPolicy{};
  }
}

BufferManagerMode get_mode_from_env() {
  if (const auto mode_string = std::getenv("HYRISE_BUFFER_MANAGER_MODE")) {
    auto mode = magic_enum::enum_cast<BufferManagerMode>(mode_string);
    if (!mode.has_value()) {
      Fail("HYRISE_BUFFER_MANAGER_MODE has an invalid value");
    } else {
      return mode.value();
    }
  } else {
    return BufferManagerMode::DramSSD;
  }
}

uint8_t get_numa_node_from_env() {
#if HYRISE_NUMA_SUPPORT
  if (const auto numa_node = std::getenv("HYRISE_BUFFER_MANAGER_NUMA_MEMORY_NODE")) {
    return boost::lexical_cast<size_t>(numa_node);
  } else {
    Fail("HYRISE_BUFFER_MANAGER_NUMA_MEMORY_NODE not found in environment");
  }
#else
  return NO_NUMA_MEMORY_NODE;
#endif
}

}  // namespace hyrise