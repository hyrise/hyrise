#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

class MetaBufferManagerMetricsTable : public AbstractMetaTable {
 public:
  MetaBufferManagerMetricsTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace hyrise
