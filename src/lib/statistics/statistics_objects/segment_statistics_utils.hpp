#pragma once

#include "types.hpp"

namespace opossum {

class Table;

void create_pruning_filter_for_chunk(Table& table, const ChunkID chunk_id);

void create_pruning_filter_for_immutable_chunks(Table& table);

}  // namespace opossum
