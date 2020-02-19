#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for settings control via a meta table.
 * Inserting only modifies an existing setting, as we onlý want to update.
 */
class MetaSettingsTable : public AbstractMetaTable {
 public:
  MetaSettingsTable();

  const std::string& name() const final;

  static bool can_update();

 protected:
  std::shared_ptr<Table> _on_generate() const;

  void _on_insert(const std::vector<AllTypeVariant>& values);
  void _on_remove(const std::vector<AllTypeVariant>& values);

  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
