#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "storage/chunk.hpp"

namespace hyrise {

class Table;

enum class FinalizeLastChunk : bool { Yes = true, No = false };

std::shared_ptr<Table> load_table(const std::string& file_name, ChunkOffset chunk_size = Chunk::DEFAULT_SIZE,
                                  FinalizeLastChunk finalize_last_chunk = FinalizeLastChunk::Yes);

/**
 * Creates an empty table based on the meta information in the first lines of the file without loading the data itself.
 */
std::shared_ptr<Table> create_table_from_header(const std::string& file_name,
                                                ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);
std::shared_ptr<Table> create_table_from_header(std::ifstream& infile, ChunkOffset chunk_size);

}  // namespace hyrise
