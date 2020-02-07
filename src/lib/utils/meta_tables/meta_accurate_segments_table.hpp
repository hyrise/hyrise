#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is class for showing information of all stored segments via a meta table.
 * To get less accurate results, use MetaSegmentsTable.
 */
class MetaAccurateSegmentsTable : public AbstractMetaTable {
 public:
  MetaAccurateSegmentsTable();

  const std::string& name() const final;

  const TableColumnDefinitions& column_definitions() const;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
