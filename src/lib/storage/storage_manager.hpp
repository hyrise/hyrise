#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "lqp_view.hpp"
#include "prepared_plan.hpp"
#include "types.hpp"

namespace opossum {

class Table;
class AbstractLQPNode;

// The StorageManager is a class that maintains all tables
// by mapping table names to table instances.
class StorageManager : public Noncopyable {
 public:
  /**
   * @defgroup Manage Tables, this is only thread-safe for operations on tables with different names
   * @{
   */
  void add_table(const std::string& name, std::shared_ptr<Table> table);
  void drop_table(const std::string& name);
  std::shared_ptr<Table> get_table(const std::string& name) const;
  bool has_table(const std::string& name) const;
  std::vector<std::string> table_names() const;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables() const;
  /** @} */

  /**
   * @defgroup Manage SQL VIEWs, this is only thread-safe for operations on views with different names
   * @{
   */
  void add_view(const std::string& name, const std::shared_ptr<LQPView>& view);
  void drop_view(const std::string& name);
  std::shared_ptr<LQPView> get_view(const std::string& name) const;
  bool has_view(const std::string& name) const;
  std::vector<std::string> view_names() const;
  std::unordered_map<std::string, std::shared_ptr<LQPView>> views() const;
  /** @} */

  /**
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on prepared plans with different names
   * @{
   */
  void add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  std::shared_ptr<PreparedPlan> get_prepared_plan(const std::string& name) const;
  bool has_prepared_plan(const std::string& name) const;
  void drop_prepared_plan(const std::string& name);
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> prepared_plans() const;
  /** @} */

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

 protected:
  StorageManager() = default;
  friend class Hyrise;

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t _INITIAL_MAP_SIZE = 100;

  tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>> _tables{_INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{_INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{_INITIAL_MAP_SIZE};
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace opossum
