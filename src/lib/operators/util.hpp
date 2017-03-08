#pragma once

class Chunk;

namespace opossum {

/**
 * This method returns false iff any of the columns are not referencing columns, or reference different tables,
 * or use different position lists.
 */
bool chunk_references_only_one_table(const Chunk& chunk);
}  // namespace opossum
