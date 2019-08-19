#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class Table;

class MetaTableManager : public Noncopyable {
 public:
  static inline const auto META_PREFIX = std::string{"meta_"};

  // Returns a sorted list of all meta table names (without prefix)
  const std::vector<std::string>& table_names() const;

  // Generates the meta table specified by table_name (which should not include the prefix)
  std::shared_ptr<Table> generate_table(const std::string& table_name) const;

  // Generator methods for the different meta tables
  std::shared_ptr<Table> generate_tables_table() const;
  std::shared_ptr<Table> generate_columns_table() const;
  std::shared_ptr<Table> generate_chunks_table() const;
  std::shared_ptr<Table> generate_segments_table() const;

  // Returns name.starts_with(META_PREFIX) as stdlibc++ does not support starts_with yet.
  bool is_meta_table_name(const std::string& name) const;

 protected:
  friend class Hyrise;
  MetaTableManager();

  std::unordered_map<std::string, std::function<std::shared_ptr<Table>(void)>> _methods;
  std::vector<std::string> _table_names;
};

}  // namespace opossum
