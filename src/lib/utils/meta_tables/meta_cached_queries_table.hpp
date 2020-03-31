#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing all cached SQL queries via a meta table.
 */
class MetaCachedQueriesTable : public AbstractMetaTable {
 public:
  MetaCachedQueriesTable();
  const std::string& name() const final;

 protected:
  friend class MetaTableManager;

  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
