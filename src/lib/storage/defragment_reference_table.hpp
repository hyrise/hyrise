#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class Table;

/**
 * Creates a new Table, merging consecutive small Chunks into one bigger Chunk if possible, to decrease the overall
 * number of Chunks. The order of rows is preserved.
 *
 * # Motivation
 * Some Operators create a lot of (potentially small) Chunks (in particular the JoinHash) which could lead to bad
 * performance. This is a utility function to achieve reasonable row counts in Chunks.
 *
 * @param reference_table   A Table with TableType::References
 * @param min_chunk_size    Any Chunk with more rows than this will not be merged with another Chunk
 * @param max_chunk_size    Max number of rows for any Chunk created as the result of merging Chunk
 * @return                  A new reference Table.
 */
std::shared_ptr<Table> defragment_reference_table(const std::shared_ptr<const Table>& reference_table,
                                                  const ChunkOffset min_chunk_size, const ChunkOffset max_chunk_size);

}  // namespace opossum
