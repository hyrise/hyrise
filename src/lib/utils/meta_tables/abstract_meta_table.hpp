#pragma once

#include "all_type_variant.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This is an abstract class for all meta table objects.
 * Meta tables are significant different from normal tables.
 * Information provided by the table is usually not persisted, but gathered on the fly.
 *
 * A meta table provides methods for both retrieving, inserting, deleting or updating
 * and the information if the operation may be done.
 *
 * Meta tables that allow the updating/deleting values should use the first column of
 * the generated table as a key.
 *
 * Meta tables should be declared as members in the MetaTableManager constructor.
 */
class AbstractMetaTable : private Noncopyable {
 public:
  AbstractMetaTable();
  virtual ~AbstractMetaTable() = default;

  virtual const std::string& name() const = 0;

  /* Generates the meta table on the fly by calling _on_generate().
   * It finalizes the last chunk of the table and sets table statistics.
   */
  const std::shared_ptr<Table> generate() const;

  const TableColumnDefinitions& column_definitions() const;

  static bool can_insert();
  static bool can_update();
  static bool can_remove();

  // TO DO: should we move all implementations mutating methods to _on_operation and add Asserts for length and types of values here?
  [[noreturn]] void insert(const std::vector<AllTypeVariant>& values);
  [[noreturn]] void update(const AllTypeVariant& key, const std::vector<AllTypeVariant>& values);
  [[noreturn]] void remove(const AllTypeVariant& key);

 protected:
  // This method actually generates the output table.
  virtual std::shared_ptr<Table> _on_generate() const = 0;
  const TableColumnDefinitions _column_definitions;
};

}  // namespace opossum
