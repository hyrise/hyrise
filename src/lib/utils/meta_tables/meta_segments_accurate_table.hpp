#pragma once

#include <memory>
#include <string>

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

/**
 * This is a class for showing information of all stored segments via a meta table. Here, we provide
 * - the distinct value count per segment, which is computed if it is not cached, and
 * - memory usage derived by iterating over all values.
 * MetaSegmentsTable provides similar information, but more efficiently. It uses only cached distinct value counts and
 * estimates the memory usage by sampling.
 */
class MetaSegmentsAccurateTable : public AbstractMetaTable {
 public:
  MetaSegmentsAccurateTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace hyrise
