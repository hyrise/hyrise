#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class Partition {
 public:
  explicit Partition(Table& table);

  void append(std::vector<AllTypeVariant> values);
  ChunkID chunk_count() const;
  void create_new_chunk();
  uint64_t row_count() const;

 protected:
  std::vector<Chunk> _chunks;
  Table& _table;
};

}  // namespace opossum
