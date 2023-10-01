#pragma once
#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include "migration_policy.hpp"
#include "scheduler/topology.hpp"
#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

/**
 * BufferManagerConfig allows to configure the BufferManager e.g pool sizes, debug parameters etc. 
 * The configuration can be loaded from a JSON file by setting the HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH environment variable.
 * If not provided, the given default parameters are used.
 * 
 * TODO: AbstractSetting: BufferManager.0.dram_buffer_pool_size ... the number is the numa node
*/

struct BufferManagerConfig {
  // Create config based on a NUMA topology and the available memory
  explicit BufferManagerConfig(const Topology& topology);

  // Create config based on the default topology
  explicit BufferManagerConfig();

  // Protect evicted pages from being written or read. This is useful for debugging memory errors (default: false)
  static constexpr bool ENABLE_MPROTECT = false;

  // Defines the total virtual memory that is reserved for the buffer pool in bytes (default: 256 GiB)
  static constexpr size_t DEFAULT_RESERVED_VIRTUAL_MEMORY = 1UL << 38;  // 256 GiB

  // Number of frames per region (default: 1M)
  static constexpr size_t INITIAL_FRAMES_PER_REGION = 1000000;

  // Defines the size of the buffer pool in DRAM in bytes (default: 80% of current free memory)
  std::size_t dram_buffer_pool_size = 1 << 30;

  // Defines the size of the buffer pool in memory node in bytes (default: 80% of current free memory if memory node is set, otherwise 0)
  std::size_t numa_buffer_pool_size = 0;

  // Defines the miration policy to use (default: lazy). See MigrationPolicy for more details.
  MigrationPolicy migration_policy = LazyMigrationPolicy;

  // Identifier of the NUMA node to use for the buffer pool (default: -1, i.e., no NUMA node)
  NodeID memory_node = INVALID_NODE_ID;

  // Primary execution node for the buffer pool. This will be ignored for non-NUMA systems (default: 0)
  NodeID cpu_node = NodeID{0};

  // Path to the SSD storage. Can be a block device or a directory. (default: WORKING_DIR/buffer_manager_data).
  std::filesystem::path ssd_path = std::filesystem::current_path() / "buffer_manager_data";

  // Load the configuration from the environment
  static BufferManagerConfig from_env_or_default(const Topology& topology);

  // Export current config to JSON
  nlohmann::json to_json() const;

  void set_default_pool_sizes(const Topology& topology);
};
}  // namespace hyrise