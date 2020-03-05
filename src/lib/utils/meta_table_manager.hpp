#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class Table;

class MetaTableManager : public Noncopyable {
 public:
  static inline const auto META_PREFIX = std::string{"meta_"};

  static bool is_meta_table_name(const std::string& name);

  // Returns a sorted list of all meta table names (without prefix)
  const std::vector<std::string>& table_names() const;

  bool has_table(const std::string& table_name) const;

  // Generates the meta table specified by table_name (which can include the prefix)
  std::shared_ptr<Table> generate_table(const std::string& table_name) const;

  bool can_insert_into(const std::string& table_name) const;
  bool can_delete_from(const std::string& table_name) const;
  bool can_update(const std::string& table_name) const;

  void insert_into(const std::string& table_name, const std::shared_ptr<const Table>& values);
  void delete_from(const std::string& table_name, const std::shared_ptr<const Table>& values);
  void update(const std::string& table_name, const std::shared_ptr<const Table>& selected_values,
              const std::shared_ptr<const Table>& update_values);

 protected:
  friend class Hyrise;
  friend class MetaTableManagerTest;
  friend class ChangeMetaTableTest;

  MetaTableManager();

  void _add(const std::shared_ptr<AbstractMetaTable>& table);
  static std::string _trim_table_name(const std::string& table_name);

  std::unordered_map<std::string, std::shared_ptr<AbstractMetaTable>> _meta_tables;
  std::vector<std::string> _table_names;
};

}  // namespace opossum
