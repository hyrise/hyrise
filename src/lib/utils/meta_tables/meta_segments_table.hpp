#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

/**
 * This is a class for showing information of all stored segments via a meta table. Here, we only provide
 * - the distinct value count per segment if it is cached by a statistics object and
 * - an estimated memory usage derived by sampling.
 * For slower results where this information is more accurate use MetaSegmentsAccurateTable.
 */
class MetaSegmentsTable : public AbstractMetaTable {
 public:
  MetaSegmentsTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace hyrise
