#pragma once

#include <map>
#include <string>
#include <vector>

#include "chunk.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {

class Table {
 public:
  explicit Table(const size_t chunk_size = 0);
  Table(Table const &) = delete;
  Table(Table &&) = default;

  size_t col_count() const;
  size_t row_count() const;
  size_t chunk_count() const;
  Chunk &get_chunk(ChunkID chunk_id);
  const std::string &get_column_name(size_t column_id) const;
  const std::string &get_column_type(size_t column_id) const;
  size_t get_column_id_by_name(const std::string &column_name) const;

  void add_column(const std::string &name, const std::string &type, bool as_value_column = true);
  void append(std::initializer_list<AllTypeVariant> values) DEV_ONLY;
  std::vector<int> column_string_widths(int max = 0) const;
  void print(std::ostream &out = std::cout) const;

 protected:
  const size_t _chunk_size;
  std::vector<Chunk> _chunks;
  std::vector<std::string> _column_names;
  std::vector<std::string> _column_types;
};
}  // namespace opossum
