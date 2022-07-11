#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * TODO
 */
class MetaTemporaryMemoryUsageTable : public AbstractMetaTable {
 public:
  MetaTemporaryMemoryUsageTable();
  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
