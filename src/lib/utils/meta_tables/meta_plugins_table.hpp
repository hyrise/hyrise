#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for plugin control via a meta table.
 * Inserting loads a plugin, deleting unloads it.
 */
class MetaPluginsTable : public AbstractMetaTable {
 public:
  MetaPluginsTable();

  const std::string& name() const final;

  bool can_insert() const final;
  bool can_delete() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;

  void _on_insert(const std::vector<AllTypeVariant>& values) final;
  void _on_remove(const std::vector<AllTypeVariant>& values) final;
};

}  // namespace opossum
