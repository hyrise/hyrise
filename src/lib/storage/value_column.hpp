#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector
template <typename T>
class ValueColumn : public BaseColumn {
 public:
  ValueColumn() = default;

  // Create a ValueColumn with the given values
  explicit ValueColumn(alloc_concurrent_vector<T>&& values);

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override;

  const T get(const size_t i) const;

  // add a value to the end
  void append(const AllTypeVariant& val) override;

  // returns all values
  const alloc_concurrent_vector<T>& values() const;
  alloc_concurrent_vector<T>& values();

  // return the number of entries
  size_t size() const override;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override;

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override;

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const override;

  const std::shared_ptr<alloc_vector<std::pair<RowID, T>>> materialize(
      ChunkID chunk_id, std::shared_ptr<alloc_vector<ChunkOffset>> offsets = nullptr);

 protected:
  alloc_concurrent_vector<T> _values;
};
}  // namespace opossum
