#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing all indexes via a meta table.
 */
class MetaIndexesTable : public AbstractMetaTable {
 public:
  MetaIndexesTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
