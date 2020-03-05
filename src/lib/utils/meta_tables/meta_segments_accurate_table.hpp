#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing information of all stored segments via a meta table.
 * To get less accurate results, use MetaSegmentsTable.
 */
class MetaSegmentsAccurateTable : public AbstractMetaTable {
 public:
  MetaSegmentsAccurateTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const;
};

}  // namespace opossum
