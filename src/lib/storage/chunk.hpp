#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_column.hpp"
#include "value_column.hpp"

namespace opossum {
class Print;
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
  size_t size() const;

  friend class Print;

 protected:
  std::vector<std::shared_ptr<BaseColumn>> _columns;
};
}  // namespace opossum
