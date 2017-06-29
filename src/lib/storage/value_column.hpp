#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector
template <typename T>
class ValueColumn : public BaseColumn {
 public:
  explicit ValueColumn(bool nullable = false);

  // Create a ValueColumn with the given values
  explicit ValueColumn(tbb::concurrent_vector<T>&& values);
  explicit ValueColumn(tbb::concurrent_vector<T>&& values, tbb::concurrent_vector<bool>&& null_values);

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override;

  const T get(const size_t i) const;

  // add a value to the end
  void append(const AllTypeVariant& val) override;

  // returns all values
  const tbb::concurrent_vector<T>& values() const;
  tbb::concurrent_vector<T>& values();

  // returns if columns supports null values
  bool is_nullable() const;

  /**
   * @brief Returns null array
   *
   * Throws exception if is_nullable() returns false
   */
  const tbb::concurrent_vector<bool>& null_values() const;
  tbb::concurrent_vector<bool>& null_values();

  // return the number of entries
  size_t size() const override;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override;

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override;

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const override;

  const std::shared_ptr<std::vector<std::pair<RowID, T>>> materialize(
      ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets = nullptr);

 protected:
  tbb::concurrent_vector<T> _values;
  optional<tbb::concurrent_vector<bool>> _null_values;
  // While a ValueColumn knows if it is nullable or not by looking at this optional, a DictionaryColumn does not.
  // For this reason, we need to store the nullable information separately in the table's definition.
};

}  // namespace opossum
