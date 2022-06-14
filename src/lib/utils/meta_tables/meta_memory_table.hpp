#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * TODO
 */
class MetaMemoryTable : public AbstractMetaTable {
 public:
  MetaMemoryTable();
  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
