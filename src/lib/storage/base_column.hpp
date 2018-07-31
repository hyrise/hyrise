#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/format_bytes.hpp"

namespace opossum {

class AbstractColumnVisitor;
class ColumnVisitorContext;

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn
class BaseColumn : private Noncopyable {
 public:
  explicit BaseColumn(const DataType data_type);
  virtual ~BaseColumn() = default;

  // the type of the data contained in this column
  DataType data_type() const;

  // returns the value at a given position
  virtual const AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // appends the value at the end of the column
  virtual void append(const AllTypeVariant& val) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // Copies a column using a new allocator. This is useful for placing the column on a new NUMA node.
  virtual std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const = 0;

  // Estimate how much memory the Column is using. Might be inaccurate, especially if the column contains non-primitive
  // data, such as strings who memory usage is implementation defined
  virtual size_t estimate_memory_usage() const = 0;

 private:
  const DataType _data_type;
};
}  // namespace opossum
