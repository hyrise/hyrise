#pragma once

#include "utils/meta_tables/abstract_meta_system_table.hpp"

namespace opossum {

/**
 * This is a class for showing information about static system properties such as hardware capabilities.
 */
class MetaSystemInformationTable : public AbstractMetaSystemTable {
 public:
  MetaSystemInformationTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate();

  const pmr_string _cpu_model() const;
};

}  // namespace opossum
