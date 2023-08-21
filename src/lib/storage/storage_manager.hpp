#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "lqp_view.hpp"
#include "prepared_plan.hpp"
#include "types.hpp"
#include "storage/chunk.hpp"
#include "memory/numa_memory_resource.hpp"

namespace hyrise {

class Table;
class AbstractLQPNode;

// The StorageManager is a class that maintains all tables
// by mapping table names to table instances.
class StorageManager : public Noncopyable {
 public:
  /**
   * @defgroup Manage Tables, this is only thread-safe for operations on tables with different names
   * @{
   */
  void add_table(const std::string& name, std::shared_ptr<Table> table);
  void drop_table(const std::string& name);
  std::shared_ptr<Table> get_table(const std::string& name) const;
  bool has_table(const std::string& name) const;
  std::vector<std::string> table_names() const;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables() const;
  /** @} */

  /**
   * @defgroup Manage SQL VIEWs, this is only thread-safe for operations on views with different names
   * @{
   */
  void add_view(const std::string& name, const std::shared_ptr<LQPView>& view);
  void drop_view(const std::string& name);
  std::shared_ptr<LQPView> get_view(const std::string& name) const;
  bool has_view(const std::string& name) const;
  std::vector<std::string> view_names() const;
  std::unordered_map<std::string, std::shared_ptr<LQPView>> views() const;
  /** @} */

  /**
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on prepared plans with different names
   * @{
   */
  void add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  std::shared_ptr<PreparedPlan> get_prepared_plan(const std::string& name) const;
  bool has_prepared_plan(const std::string& name) const;
  void drop_prepared_plan(const std::string& name);
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> prepared_plans() const;
  /** @} */

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

  //
  // NUMA Memory Management
  //

  /**
   * Builds one NUMA memory resource for each NUMA node. Currently, this is done by
   * using Jemalloc, which holds one arena per memory resource. 
   */
  void build_memory_resources();

  size_t number_of_memory_resources();
  NumaMemoryResource* get_memory_resource(NodeID node_id);

  /**
   * Migrates the tables and their chunks to their desired memory resources.
   */
  void migrate_table(std::shared_ptr<Table> table, NodeID target_node_id);
  void migrate_chunk(std::shared_ptr<Chunk> chunk, NodeID target_node_id);

  static void* alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero,
                    bool* commit, unsigned arena_index);
  void store_node_id_for_arena(ArenaID, NodeID);

  extent_hooks_t* get_extent_hooks();
  std::unordered_map<ArenaID, NodeID> node_id_for_arena_id;

  // It is static, so that it lives until the process terminates.
  // Necessary, in order to allow jemalloc to deallocate as long as possible.
  // TODO(anyone): Find a way not to make it static.
  inline static std::vector<NumaMemoryResource> memory_resources;

 protected:
  StorageManager() = default;
  friend class Hyrise;

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t INITIAL_MAP_SIZE = 100;

  tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>> _tables{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{INITIAL_MAP_SIZE};

  inline static extent_hooks_t _hooks;
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
