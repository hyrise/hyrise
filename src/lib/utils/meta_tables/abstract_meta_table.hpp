#pragma once

#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This is an abstract class for all meta table objects.
 * Meta tables are significantly different from normal tables.
 * Information provided by the table is usually not persisted, but gathered on the fly.
 *
 * A meta table may provide methods for retrieving, inserting, deleting or updating.
 * Not all of these operations are allowed for all meta tables. Therefore, they also provide methods
 * that indicate whether the particular operation is allowed, e.g., can_insert().
 *
 * Modifications of a meta table's values usually trigger some actions, like loading plugins.
 *
 * Meta tables should be added in the MetaTableManager constructor body.
 */
class AbstractMetaTable : public Noncopyable {
 public:
  virtual const std::string& name() const = 0;

  const TableColumnDefinitions& column_definitions() const;

  virtual bool can_insert() const;
  virtual bool can_update() const;
  virtual bool can_delete() const;

 protected:
  friend class MetaTableManager;
  friend class MetaTableManagerTest;
  friend class MetaTableTest;
  friend class MetaPluginsTest;
  friend class MetaSettingsTest;

  explicit AbstractMetaTable(const TableColumnDefinitions& column_definitions);

  virtual ~AbstractMetaTable() = default;

  /*
   * Generates the meta table on the fly by calling _on_generate().
   * It finalizes the last chunk of the table and sets table statistics.
   */
  std::shared_ptr<Table> _generate() const;

  /*
   * Manipulates the meta table by calling _on_insert() / _on_remove.
   * Additionally, checks if the input values match the column definitions.
   */
  void _insert(const std::vector<AllTypeVariant>& values);
  void _remove(const std::vector<AllTypeVariant>& values);
  void _update(const std::vector<AllTypeVariant>& selected_values, const std::vector<AllTypeVariant>& update_values);

  void _validate_data_types(const std::vector<AllTypeVariant>& values) const;

  // These methods actually perform the table creation and manipulation.
  virtual std::shared_ptr<Table> _on_generate() const = 0;
  virtual void _on_insert(const std::vector<AllTypeVariant>& values);
  virtual void _on_remove(const std::vector<AllTypeVariant>& values);
  virtual void _on_update(const std::vector<AllTypeVariant>& selected_values,
                          const std::vector<AllTypeVariant>& update_values);

  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
