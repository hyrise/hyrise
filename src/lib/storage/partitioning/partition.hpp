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

/*
 * A Partition is a logical horizontal slice of a Table.
 * It is one level of abstraction above Chunks,
 * hence a Partition holds vector of shared_ptr's to Chunks of an associated Table.
 * The Table knows its PartitionSchema knowing its Partitions but there is no reference back.
 * All tuples in a Partition share a common property defined by the PartitionSchema,
 * holding a number of Partitions.
 */

class Partition {
 public:
  Partition() = default;

  void clear();
  void add_new_chunk(std::shared_ptr<Chunk> chunk);

  // adds a new row, given as a list of values, to the last chunk of the partition
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(const std::vector<AllTypeVariant>& values);

  std::vector<std::shared_ptr<const Chunk>> get_chunks() const;
  std::shared_ptr<Chunk> last_chunk() const;

  Partition(Partition&&) = default;
  Partition& operator=(Partition&&) = default;

 protected:
  std::vector<std::shared_ptr<Chunk>> _chunks;
};

}  // namespace opossum
