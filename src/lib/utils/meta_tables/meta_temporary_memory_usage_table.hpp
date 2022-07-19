#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing temporary memory usage in a meta table.
 *
 * Temporary memory usage refers to memory allocated by operators during their execution time, 
 * i.e. the memory used by operator-internal data structures. The data is collected using
 * polymorphic allocators with memory resources that track allocations and deallocations. 
 */
class MetaTemporaryMemoryUsageTable : public AbstractMetaTable {
 public:
  MetaTemporaryMemoryUsageTable();
  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
