#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing all stored columns via a meta table.
 */
class MetaColumnsTable : public AbstractMetaTable {
 public:
  MetaColumnsTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const;
};

}  // namespace opossum
