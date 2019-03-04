#pragma once

#include <memory>
#include <unordered_set>

namespace opossum {

class Table;

/**
 * Generate Pruning Filter for all immutable Chunks in this Table
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace opossum
