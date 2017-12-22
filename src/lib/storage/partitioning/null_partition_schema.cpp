#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>()); }

}  // namespace opossum
