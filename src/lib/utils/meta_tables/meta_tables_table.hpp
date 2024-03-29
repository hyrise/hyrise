#pragma once

#include <memory>
#include <string>

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

/**
 * This is a class for showing all stored tables via a meta table.
 */
class MetaTablesTable : public AbstractMetaTable {
 public:
  MetaTablesTable();
  const std::string& name() const final;

 protected:
  friend class MetaTableManager;

  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace hyrise
