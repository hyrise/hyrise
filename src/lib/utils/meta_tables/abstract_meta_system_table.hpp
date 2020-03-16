#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This is a class for showing information about static system properties such as hardware capabilities.
 */
class AbstractMetaSystemTable : public AbstractMetaTable {
 public:
  virtual const std::string& name() const = 0;

 protected:
 	AbstractMetaSystemTable(const TableColumnDefinitions& column_definitions);
 	int _get_cpu_count();
};

}  // namespace opossum
