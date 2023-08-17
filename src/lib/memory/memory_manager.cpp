#include "memory/memory_manager.hpp"
#include "hyrise.hpp"

namespace hyrise {

void MemoryManager::build_memory_resources() {
  const auto num_nodes = static_cast<NodeID>(Hyrise::get().topology.nodes().size());
  _memory_resources.resize(num_nodes);
  for (auto node_id = NodeID{0}; node_id < num_nodes; ++node_id) {
    const auto numa_memory_resource = NumaMemoryResource(node_id);
    _memory_resources.push_back(numa_memory_resource);
  }
}

void MemoryManager::migrate_table(std::shared_ptr<Table> table, NodeID target_node_id) {
  const auto memory_resource = _memory_resources.at(target_node_id).get();
  const auto chunk_count = table->chunk_count();

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto migrate_job = [&table, &memory_resource, chunk_id, target_node_id]() {
      const auto& chunk = table->get_chunk(chunk_id);
      chunk->migrate(memory_resource, target_node_id);
    };
    jobs.emplace_back(std::make_shared<JobTask>(migrate_job));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

MemoryManager::number_of_memory_resources() {
  return _memory_resources.size();
}

MemoryManager::MemoryManager() {
  build_memory_resources();
}

}  // namespace hyrise
