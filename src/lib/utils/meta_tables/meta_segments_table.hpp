#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing information of all stored segments via a meta table.
 * To get more accurate results, use MetaAccurateSegmentsTable.
 */
class MetaSegmentsTable : public AbstractMetaTable {
 public:
  MetaSegmentsTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
