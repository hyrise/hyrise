#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "table.hpp"

namespace opossum {

// The StorageManager is a singleton that maintains all tables
// by mapping table names to table instances.
class StorageManager {
 public:
  static StorageManager &get();

  // adds a table to the storage manager
  void add_table(const std::string &name, std::shared_ptr<Table> table);

  // removes the table from the storage manger
  void drop_table(const std::string &name);

  // returns the table instance with the given name
  std::shared_ptr<Table> get_table(const std::string &name) const;

  // returns whether the storage manager holds a table with the given name
  bool has_table(const std::string &name) const;

  // prints the table on the given stream
  void print(std::ostream &out = std::cout) const;

  // deletes the entire StorageManager and creates a new one, used especially in tests
  static void reset();

  // For debugging purposes mostly, dump all tables as csv
  void dump_as_csv(const std::string &path);

  StorageManager(StorageManager const &) = delete;
  StorageManager &operator=(const StorageManager &) = delete;
  StorageManager(StorageManager &&) = delete;

 protected:
  StorageManager() {}
  StorageManager &operator=(StorageManager &&) = default;

  std::map<std::string, std::shared_ptr<Table>> _tables;
};
}  // namespace opossum
