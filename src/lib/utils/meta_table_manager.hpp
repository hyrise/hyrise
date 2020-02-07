#pragma once

#include <functional>
#include <unordered_map>

#include "boost/tuple/tuple.hpp"

#include "types.hpp"

namespace opossum {

class Table;

class MetaTableManager : public Noncopyable {
 public:
  static inline const auto META_PREFIX = std::string{"meta_"};

  // Returns a sorted list of all meta table names (without prefix)
  const std::vector<std::string>& table_names() const;

  bool has_table(const std::string& table_name) const;
  bool table_is_mutable(const std::string& table_name) const;

  void insert_into(const std::string& table_name, const std::string& value);
  void delete_from(const std::string& table_name, const std::string& value);

  // Generates the meta table specified by table_name (which should not include the prefix)
  std::shared_ptr<Table> generate_table(const std::string& table_name) const;

  // Generator methods for the different meta tables
  static std::shared_ptr<Table> generate_tables_table();
  static std::shared_ptr<Table> generate_columns_table();
  static std::shared_ptr<Table> generate_chunks_table();
  static std::shared_ptr<Table> generate_chunk_sort_orders_table();
  static std::shared_ptr<Table> generate_segments_table();
  static std::shared_ptr<Table> generate_accurate_segments_table();
  static std::shared_ptr<Table> generate_plugins_table();

  // Returns name.starts_with(META_PREFIX) as stdlibc++ does not support starts_with yet.
  static bool is_meta_table_name(const std::string& name);

 protected:
  friend class Hyrise;
  MetaTableManager();

  static void _insert_into_plugins(const std::string& value);
  static void _delete_from_plugins(const std::string& value);

  std::unordered_map<std::string, std::function<std::shared_ptr<Table>(void)>> _methods;
  std::unordered_map<std::string,
                     std::array<std::function<std::shared_ptr<Table>(const std::string&)>, 2>> _mutating_methods;

                         //std::function<std::shared_ptr<Table>(const std::string&)>,
                         //std::function<std::shared_ptr<Table>(const std::string&)>>>
      //_mutating_methods;

  std::vector<std::string> _table_names;
  std::vector<std::string> _mutable_table_names;
};

}  // namespace opossum
