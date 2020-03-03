#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for settings control via a meta table.
 * We only want to update setting values with this.
 */
class MetaSettingsTable : public AbstractMetaTable {
 public:
  MetaSettingsTable();

  const std::string& name() const final;

  bool can_update() const;

 protected:
  friend class MetaSettingsTest;
  std::shared_ptr<Table> _on_generate() const;

  void _on_update(const std::vector<AllTypeVariant>& selected_values, const std::vector<AllTypeVariant>& update_values);
};

}  // namespace opossum
