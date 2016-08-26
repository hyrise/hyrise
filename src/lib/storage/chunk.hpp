#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_column.hpp"
#include "value_column.hpp"

namespace opossum {

class Chunk {
 public:
  Chunk();
  explicit Chunk(const std::vector<std::string> &column_types);
  Chunk(const Chunk &) = delete;
  Chunk(Chunk &&) = default;

  void add_column(std::string type);
  void add_column(std::shared_ptr<BaseColumn> column);
  void append(std::initializer_list<AllTypeVariant> values) DEV_ONLY;
  std::shared_ptr<BaseColumn> get_column(size_t column_id) const;
  std::vector<int> column_string_widths(int max = 0) const;
  void print(std::ostream &out = std::cout, const std::vector<int> &column_string_widths = std::vector<int>()) const;
  size_t size() const;

 protected:
  std::vector<std::shared_ptr<BaseColumn>> _columns;
};
}  // namespace opossum
