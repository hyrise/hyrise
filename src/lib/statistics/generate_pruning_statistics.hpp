#pragma once

#include <memory>
#include <unordered_set>

namespace hyrise {

class Chunk;
class Table;

/**
 * Check whether a chunk is immutable and has no pruning statistics (we only want to generate statistics if both
 * conditions are true).
 */
bool is_immutable_chunk_without_pruning_statistics(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for an immutable Chunk
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for all immutable Chunks in this Table
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace hyrise
