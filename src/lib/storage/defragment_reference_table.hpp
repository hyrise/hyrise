#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class Table;

std::shared_ptr<Table> defragment_reference_table(const std::shared_ptr<const Table>& reference_table, const ChunkOffset min_chunk_size, const ChunkOffset max_chunk_size);

}  // namespace opossum
