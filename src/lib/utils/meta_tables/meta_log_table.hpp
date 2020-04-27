#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class MetaLogTable : public AbstractMetaTable {
 public:
  MetaLogTable();

  const std::string& name() const final;

 protected:
  friend class MetaLogTest;
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
