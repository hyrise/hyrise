#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is class for showing all stored chunks via a meta table.
 */
class MetaChunksTable : public AbstractMetaTable {
 public:
  explicit MetaChunksTable();

  const std::string& name() const final;

  const TableColumnDefinitions& column_definitions() const;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
