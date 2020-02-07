#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is class for showing all sort orders of stored chunks via a meta table.
 */
class MetaChunkOrdersTable : public AbstractMetaTable {
 public:
  explicit MetaChunkOrdersTable();

  const std::string& name() const final;

  const TableColumnDefinitions& column_definitions() const;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
