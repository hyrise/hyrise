#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

/**
 * This is a class for showing information of all stored segments via a meta table. Here, we provide
 * - the distinct value count per segment, which is computed if it is not cached, and
 * - memory usage derived by iterating over all values.
 * For faster results where this information is less accurate use MetaSegmentsAccurateTable.
 */
class MetaSegmentsAccurateTable : public AbstractMetaTable {
 public:
  MetaSegmentsAccurateTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace hyrise
