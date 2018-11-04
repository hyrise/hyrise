#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "lqp_view.hpp"
#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Table;
class AbstractLQPNode;

// The StorageManager is a singleton that maintains all tables
// by mapping table names to table instances.
class StorageManager : public Singleton<StorageManager> {
 public:
  // adds a table to the storage manager
  void add_table(const std::string& name, std::shared_ptr<Table> table);

  // removes the table from the storage manger
  void drop_table(const std::string& name);

  // returns the table instance with the given name
  std::shared_ptr<Table> get_table(const std::string& name) const;

  // returns whether the storage manager holds a table with the given name
  bool has_table(const std::string& name) const;

  // returns a list of all table names
  std::vector<std::string> table_names() const;

  // returns a map from table name to table
  const std::map<std::string, std::shared_ptr<Table>>& tables() const;

  // adds a view to the storage manager
  void add_lqp_view(const std::string& name, const std::shared_ptr<LQPView>& view);

  // removes the view from the storage manger
  void drop_lqp_view(const std::string& name);

  // returns the view instance with the given name
  std::shared_ptr<LQPView> get_view(const std::string& name) const;

  // returns whether the storage manager holds a table with the given name
  bool has_view(const std::string& name) const;

  // returns a list of all view names
  std::vector<std::string> view_names() const;

  // prints information about all tables in the storage manager (name, #columns, #rows, #chunks)
  void print(std::ostream& out = std::cout) const;

  // deletes the entire StorageManager and creates a new one, used especially in tests
  // This can lead to a lot of issues if there are still running tasks / threads that
  // want to access a resource. You should be very sure that this is what you want.
  // Have a look at base_test.hpp to see the correct order of resetting things.
  static void reset();

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

  StorageManager(StorageManager&&) = delete;

 protected:
  StorageManager() {}

  friend class Singleton;

  const StorageManager& operator=(const StorageManager&) = delete;
  StorageManager& operator=(StorageManager&&) = default;

  std::map<std::string, std::shared_ptr<Table>> _tables;
  std::map<std::string, std::shared_ptr<LQPView>> _views;
};
}  // namespace opossum
