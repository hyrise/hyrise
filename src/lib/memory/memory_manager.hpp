#pragma once

#include "types.hpp"
#include "scheduler/job_task.hpp"
#include "storage/table.hpp"

namespace hyrise {

/**
 * The memory manager is responsible for moving memory to their dedicated NUMA nodes. 
 */
class MemoryManager final : public Noncopyable {
 public:
  /**
   * Builds one NUMA memory resource for each NUMA node. Currently, this is done by
   * using Jemalloc, which holds one arena per memory resource. 
   */
  void build_memory_resources();

  /**
   * Migrates the tables and their chunks to their desired memory resources.
   */
  void migrate_table(std::shared_ptr<Table> table, NodeID target_node_id);

 private:
  MemoryManager();
  friend class Hyrise;

  std::vector<std::shared_ptr<NumaMemoryResource>> _memory_resources;
};

}  // namespace hyrise
