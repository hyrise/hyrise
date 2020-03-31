#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_value_segment.hpp"
#include "chunk.hpp"

namespace opossum {

// ValueSegment is a specific segment type that stores all its values in a vector.
template <typename T>
class ValueSegment : public BaseValueSegment {
 public:
  explicit ValueSegment(bool nullable = false, ChunkOffset capacity = Chunk::DEFAULT_SIZE);

  // Create a ValueSegment with the given values.
  explicit ValueSegment(pmr_vector<T>&& values);
  explicit ValueSegment(pmr_vector<T>&& values, pmr_vector<bool>&& null_values);

  // Return the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Returns whether a value is NULL
  bool is_null(const ChunkOffset chunk_offset) const;

  // return the value at a certain position.
  // Only use if you are certain that no null values are present, otherwise an Assert fails.
  T get(const ChunkOffset chunk_offset) const;

  // return the value at a certain position.
  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    // Column supports null values and value is null
    if (is_nullable() && (*_null_values)[chunk_offset]) {
      return std::nullopt;
    }
    return _values[chunk_offset];
  }

  // Add a value to the end of the segment. Not thread-safe. May fail if ValueSegment was not initially created with
  // sufficient capacity.
  void append(const AllTypeVariant& val) final;

  // Return all values. This is the preferred method to check a value at a certain index. Usually you need to
  // access more than a single value anyway.
  // e.g. auto& values = segment.values(); and then: values.at(i); in your loop.
  const pmr_vector<T>& values() const;
  pmr_vector<T>& values();

  // Return whether segment supports null values.
  bool is_nullable() const final;

  // Return null value vector that indicates whether a value is null with true at position i.
  // Throws exception if is_nullable() returns false
  // This is the preferred method to check a for a null value at a certain index.
  // Usually you need to access more than a single value anyway.
  const pmr_vector<bool>& null_values() const final;

  // Writing a vector<bool> is not thread-safe. By only exposing the vector as a const reference, we force people to go
  // through this thread-safe method. By design, this does not take a bool argument. All entries are false (i.e., not
  // NULL) by default. Setting them to false again is unnecessarily expensive and changing them from true to false
  // should never be necessary.
  void set_null_value(const ChunkOffset chunk_offset);

  // Return the number of entries in the segment.
  ChunkOffset size() const final;

  // Resizes the ValueSegment, but does not allocate new memory. This is used by the Insert operator instead of
  // immediately resizing the vector. The reason for that is that the thread sanitizer does not understand atomic
  // resizes and our MVCC concept. As such, this method is part of tsan-ignore.txt.
  void resize(const size_t size);

  // Copies a ValueSegment using a new allocator. This is useful for placing the ValueSegment on a new NUMA node.
  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const override;

 protected:
  pmr_vector<T> _values;
  std::optional<pmr_vector<bool>> _null_values;

  // Protects set_null_value. Does not need to be acquired for reads, as we expect modifications to vector<bool> to be
  // atomic.
  std::mutex _null_value_modification_mutex;
};

}  // namespace opossum
