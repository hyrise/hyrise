#pragma once

#include <functional>
#include <unordered_map>

#include "utils/meta_tables/abstract_meta_table.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class MetaTableManager : public Noncopyable {
 public:
  static inline const auto META_PREFIX = std::string{"meta_"};

  // Returns name.starts_with(META_PREFIX) as stdlibc++ does not support starts_with yet.
  static bool is_meta_table_name(const std::string& name);

  // Returns a sorted list of all meta table names (without prefix)
  const std::vector<std::string>& table_names() const;

  bool has_table(const std::string& table_name) const;

  // Generates the meta table specified by table_name (which should not include the prefix)
  std::shared_ptr<Table> generate_table(const std::string& table_name) const;

  bool can_insert_into(const std::string& table_name) const;
  bool can_delete_from(const std::string& table_name) const;
  bool can_update(const std::string& table_name) const;

  void insert_into(const std::string& table_name, const std::shared_ptr<Table>& values) const;
  void delete_from(const std::string& table_name, const std::shared_ptr<Table>& values) const;
  void update(const std::string& table_name, const std::shared_ptr<Table>& fields, const std::shared_ptr<Table>& values) const;

 protected:
  friend class Hyrise;
  MetaTableManager();

  std::unordered_map<std::string, AbstractMetaTable*> _meta_tables;
  std::vector<std::string> _table_names;
};

}  // namespace opossum
