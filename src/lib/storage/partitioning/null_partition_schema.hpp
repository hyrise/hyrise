#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "storage/proxy_chunk.hpp"

namespace opossum {

class NullPartitionSchema : public PartitionSchema {
 public:
  NullPartitionSchema();

  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) override;

  // Return false, since NullPartitioningSchema is only a list of chunks.
  bool is_partitioned() const override { return false; }
};

}  // namespace opossum
