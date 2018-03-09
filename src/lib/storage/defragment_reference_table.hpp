#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class Table;

std::shared_ptr<Table> merge_adjacent_chunks(const std::shared_ptr<Table>& input_table, const ChunkOffset min_chunk_size, const ChunkOffset max_chunk_size);

}  // namespace opossum
