#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_value_segment.hpp"

namespace opossum {

// ValueSegment is a specific segment type that stores all its values in a vector.
template <typename T>
class ValueSegment : public BaseValueSegment {
 public:
  explicit ValueSegment(bool nullable = false);
  explicit ValueSegment(const PolymorphicAllocator<T>& alloc, bool nullable = false);

  // Create a ValueSegment with the given values.
  explicit ValueSegment(pmr_concurrent_vector<T>&& values, const PolymorphicAllocator<T>& alloc = {});
  explicit ValueSegment(pmr_concurrent_vector<T>&& values, pmr_concurrent_vector<bool>&& null_values,
                        const PolymorphicAllocator<T>& alloc = {});
  explicit ValueSegment(const std::vector<T>& values, const PolymorphicAllocator<T>& alloc = {});
  explicit ValueSegment(const std::vector<T>& values, std::vector<bool>& null_values,
                        const PolymorphicAllocator<T>& alloc = {});

  // Return the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Returns whether a value is NULL
  bool is_null(const ChunkOffset chunk_offset) const;

  // return the value at a certain position.
  // Only use if you are certain that no null values are present, otherwise an Assert fails.
  const T get(const ChunkOffset chunk_offset) const;

  // return the value at a certain position.
  const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  // Add a value to the end of the segment.
  void append(const AllTypeVariant& val) final;

  // Allocate enough space to hold at least @param capacity entries
  void reserve(const size_t capacity) final;

  // Return all values. This is the preferred method to check a value at a certain index. Usually you need to
  // access more than a single value anyway.
  // e.g. auto& values = segment.values(); and then: values.at(i); in your loop.
  const pmr_concurrent_vector<T>& values() const;
  pmr_concurrent_vector<T>& values();

  // Return whether segment supports null values.
  bool is_nullable() const final;

  // Return null value vector that indicates whether a value is null with true at position i.
  // Throws exception if is_nullable() returns false
  // This is the preferred method to check a for a null value at a certain index.
  // Usually you need to access more than a single value anyway.
  const pmr_concurrent_vector<bool>& null_values() const final;
  pmr_concurrent_vector<bool>& null_values() final;

  // Return the number of entries in the segment.
  size_t size() const final;

  // Copies a ValueSegment using a new allocator. This is useful for placing the ValueSegment on a new NUMA node.
  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t estimate_memory_usage() const override;

 protected:
  pmr_concurrent_vector<T> _values;

  // While a ValueSegment knows if it is nullable or not by looking at this optional, most other segment types
  // (e.g. DictionarySegment) do not. For this reason, we need to store the nullable information separately
  // in the table's definition.
  std::optional<pmr_concurrent_vector<bool>> _null_values;
};

}  // namespace opossum
