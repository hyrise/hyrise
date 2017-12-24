#pragma once

#include <all_type_variant.hpp>
#include <storage/partitioning/partition.hpp>
#include <types.hpp>

namespace opossum {

class PartitionSchema {
 public:
  // Return false for all partition schemas except NullPartitonSchema.
  // This is for performance in insert operations, which are faster for unpartitioned tables.
  virtual bool is_continous() const { return false; }

  virtual void append(std::vector<AllTypeVariant> values) = 0;
  virtual ChunkID chunk_count() const = 0;
  virtual TableType get_type() const = 0;
  virtual uint32_t max_chunk_size() const = 0;
  virtual uint64_t row_count() const = 0;

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
};

}
