#pragma once

#include <memory>
#include <unordered_set>

namespace opossum {

class Chunk;
class Table;

/**
 * Generate Pruning Filters for an immutable Chunk
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for all immutable Chunks in this Table
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace opossum
