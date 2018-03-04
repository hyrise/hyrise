#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "storage/chunk.hpp"

namespace opossum {

class Table;

template <typename T>
std::vector<T> _split(const std::string& str, char delimiter) {
  std::vector<T> internal;
  std::stringstream ss(str);
  std::string tok;

  while (std::getline(ss, tok, delimiter)) {
    internal.push_back(tok);
  }

  return internal;
}

std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size = Chunk::MAX_SIZE);

}  // namespace opossum
