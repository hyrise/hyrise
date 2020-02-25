#pragma once

#include "all_type_variant.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This is an abstract class for all meta table objects.
 * Meta tables are significant different from normal tables.
 * Information provided by the table is usually not persisted, but gathered on the fly.
 *
 * A meta table provides methods for retrieving, inserting, deleting or updating
 * and the information if the operation may be done.
 *
 * Meta tables should be declared as members in the MetaTableManager constructor.
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

  explicit AbstractMetaTable(const TableColumnDefinitions& column_definitions);

  virtual ~AbstractMetaTable() = default;

  /*
   * Generates the meta table on the fly by calling _on_generate().
   * It finalizes the last chunk of the table and sets table statistics.
   */
  const std::shared_ptr<Table> generate() const;

  /*
   * Manipulate the meta table by calling _on_insert() / _on_remove.
   * Additionally, checks if the input values match the column definitions are done.
   */
  void insert(const std::vector<AllTypeVariant>& values);
  void remove(const std::vector<AllTypeVariant>& values);
  void update(const std::vector<AllTypeVariant>& selected_values, const std::vector<AllTypeVariant>& update_values);

  void _assert_data_types(const std::vector<AllTypeVariant>& values) const;

  // These methods actually perform the table creation and manipulation.
  virtual std::shared_ptr<Table> _on_generate() const = 0;
  virtual void _on_insert(const std::vector<AllTypeVariant>& values);
  virtual void _on_remove(const std::vector<AllTypeVariant>& values);
  virtual void _on_update(const std::vector<AllTypeVariant>& selected_values,
                          const std::vector<AllTypeVariant>& update_values);

  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
