#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is class for plugin control via a meta table.
 * Inserting loads a plugin, deleting unloads it.
 */
class MetaPluginsTable : public AbstractMetaTable {
 public:
  MetaPluginsTable();

  const std::string& name() const final;

  const TableColumnDefinitions& column_definitions() const;

  static bool can_insert();
  static bool can_remove();

 protected:
  std::shared_ptr<Table> _on_generate() const;

  void _insert(const std::vector<AllTypeVariant>& values);
  void _remove(const AllTypeVariant& key);

  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
