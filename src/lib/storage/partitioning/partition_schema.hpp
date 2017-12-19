#pragma once

#include <types.hpp>
#include <all_type_variant.hpp>
#include <storage/partitioning/partition.hpp>

namespace opossum {

class PartitionSchema {

protected:
  std::vector<std::shared_ptr<Partition>> _partitions;

};

}
