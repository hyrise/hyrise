#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class ColumnVisitable;
class ColumnVisitableContext;

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn
class BaseColumn : private Noncopyable {
 public:
  BaseColumn() = default;
  virtual ~BaseColumn() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseColumn(BaseColumn&&) = default;
  BaseColumn& operator=(BaseColumn&&) = default;

  // returns the value at a given position
  virtual const AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // appends the value at the end of the column
  virtual void append(const AllTypeVariant& val) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // calls the column-specific handler in an operator (visitor pattern)
  virtual void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const = 0;

  // Copies a column using a new allocator. This is useful for placing the column on a new NUMA node.
  virtual std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const = 0;
};
}  // namespace opossum
