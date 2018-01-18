#pragma once

#include "all_type_variant.hpp"
#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/proxy_chunk.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class Partition {
 public:
  explicit Partition(PartitionID partition_id);

  const PartitionID get_partition_id();

  void clear();
  void add_new_chunk(std::shared_ptr<Chunk> chunk);
  void append(std::vector<AllTypeVariant> values);

  std::vector<std::shared_ptr<Chunk>> get_chunks() const;
  std::shared_ptr<Chunk> last_chunk();

  Partition(Partition&&) = default;
  Partition& operator=(Partition&&) = default;

 protected:
  std::vector<std::shared_ptr<Chunk>> _chunks;
  const PartitionID _partition_id;
};

}  // namespace opossum
