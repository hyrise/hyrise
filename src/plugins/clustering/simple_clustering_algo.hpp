#pragma once

#include <string>
#include <memory>

#include "abstract_clustering_algo.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class SimpleClusteringAlgo : AbstractClusteringAlgo {
 public:

  SimpleClusteringAlgo(StorageManager& storage_manager, ClusteringByTable clustering);

  const std::string description() const override;

  void run() override;

 protected:

  std::shared_ptr<Table> _sort_table_mutable(const std::shared_ptr<Table> table, const std::string& column_name, const ChunkOffset chunk_size);

  std::shared_ptr<Table> _sort_table_chunkwise(const std::shared_ptr<const Table> table, const std::string& column_name, const uint64_t desired_chunk_split_count);

  void _append_chunks(const std::shared_ptr<const Table> from, std::shared_ptr<Table> to);

  void _append_chunk(const std::shared_ptr<const Chunk> from, std::shared_ptr<Table> to);

  void _run_assertions() const;
};

} // namespace opossum