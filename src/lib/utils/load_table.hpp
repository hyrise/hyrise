#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "storage/chunk.hpp"

namespace opossum {

class Table;

std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size = Chunk::DEFAULT_SIZE, bool finalize_last_chunk = true);

/**
 * Creates an empty table based on the meta information in the first lines of the file without loading the data itself.
 */
std::shared_ptr<Table> create_table_from_header(const std::string& file_name, size_t chunk_size = Chunk::DEFAULT_SIZE);
std::shared_ptr<Table> create_table_from_header(std::ifstream& infile, size_t chunk_size);

}  // namespace opossum
