#pragma once

#include <storage/partitioning/partition_schema.hpp>

namespace opossum {

class NullPartitionSchema : public PartitionSchema {
 public:
  NullPartitionSchema();
};

}  // namespace opossum
