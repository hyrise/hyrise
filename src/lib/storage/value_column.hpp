#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_value_column.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector.
template <typename T>
class ValueColumn : public BaseValueColumn {
 public:
  explicit ValueColumn(bool nullable = false);
  explicit ValueColumn(const PolymorphicAllocator<T>& alloc, bool nullable = false);

  // Create a ValueColumn with the given values.
  explicit ValueColumn(pmr_concurrent_vector<T>&& values);
  explicit ValueColumn(pmr_concurrent_vector<T>&& values, pmr_concurrent_vector<bool>&& null_values);

  // Return the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  const AllTypeVariant operator[](const size_t i) const override;

  // Only use if you are certain that no null values are present, otherwise an Assert fails.
  const T get(const size_t i) const;

  // Add a value to the end of the column.
  void append(const AllTypeVariant& val) override;

  // Return all values. This is the preferred method to check a value at a certain index. Usually you need to
  // access more than a single value anyway.
  // e.g. auto& values = col.values(); and then: values.at(i); in your loop.
  const pmr_concurrent_vector<T>& values() const;
  pmr_concurrent_vector<T>& values();

  // Return whether column supports null values.
  bool is_nullable() const final;

  // Return null value vector that indicates whether a value is null with true at position i.
  // Throws exception if is_nullable() returns false
  // This is the preferred method to check a for a null value at a certain index.
  // Usually you need to access more than a single value anyway.
  const pmr_concurrent_vector<bool>& null_values() const final;
  pmr_concurrent_vector<bool>& null_values() final;

  // Return the number of entries in the column.
  size_t size() const override;

  // Visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const override;

  // Write the length and value at the chunk_offset to the end of row_string.
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override;

  // Copy own value to a different ValueColumn - mainly used for materialization.
  // We cannot always use the materialize method below because sort results might come from different BaseColumns.
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const override;

 protected:
  pmr_concurrent_vector<T> _values;
  std::optional<pmr_concurrent_vector<bool>> _null_values;
  // While a ValueColumn knows if it is nullable or not by looking at this optional, a DictionaryColumn does not.
  // For this reason, we need to store the nullable information separately in the table's definition.
};

}  // namespace opossum
