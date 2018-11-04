#pragma once

#include <memory>
#include <string>
#include <map>
#include <tuple>

#include "types.hpp"
#include "storage/chunk.hpp"

namespace opossum {

class Table;

std::shared_ptr<Table> load_table_cached(const std::string& file_name, size_t chunk_size = Chunk::MAX_SIZE);

}  // namespace opossum
