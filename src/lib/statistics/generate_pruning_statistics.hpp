#pragma once

#include <memory>
#include <unordered_set>

namespace hyrise {

class Chunk;
class Table;

/**
 * Check whether we need to generate pruning statistics for a chunk.
 */
bool pruning_statistics_should_be_generated_for_chunk(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for an immutable Chunk
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for all immutable Chunks in this Table
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace hyrise
