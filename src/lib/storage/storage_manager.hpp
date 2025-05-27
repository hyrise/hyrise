#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.
#include <oneapi/tbb/concurrent_vector.h>         // NOLINT(build/include_order): Identified as C system headers.

#include "storage/catalog.hpp"
#include "storage/lqp_view.hpp"
#include "storage/prepared_plan.hpp"
#include "types.hpp"

namespace hyrise {

class Table;
class AbstractLQPNode;

// The StorageManager is a class that maintains all tables by mapping table IDs to table instances.
class StorageManager : public Noncopyable {
 public:
  /**
   * @defgroup Manage tables, this is only thread-safe for operations on tables with different names.
   * @{
   */
  void add_table(const TableID table_id, std::shared_ptr<Table> table);
  void add_table(const std::string& name, std::shared_ptr<Table> table);
  void drop_table(const TableID table_id);
  void drop_table(const std::string& name);
  std::shared_ptr<Table> get_table(const TableID table_id) const;
  std::shared_ptr<Table> get_table(const std::string& name) const;
  bool has_table(const TableID table_id) const;
  bool has_table(const std::string& name) const;
  static std::vector<std::string_view> table_names();
  std::unordered_map<std::string_view, std::shared_ptr<Table>> tables() const;
  /** @} */

  /**
   * @defgroup Manage SQL views, this is only thread-safe for operations on views with different names.
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
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on
   *           prepared plans with different names.
   * @{
   */
  void add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  std::shared_ptr<PreparedPlan> get_prepared_plan(const std::string& name) const;
  bool has_prepared_plan(const std::string& name) const;
  void drop_prepared_plan(const std::string& name);
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> prepared_plans() const;
  /** @} */

  // For debugging purposes mostly, dump all tables as csv.
  void export_all_tables_as_csv(const std::string& path);

 protected:
  StorageManager() = default;
  friend class Hyrise;

  tbb::concurrent_vector<std::shared_ptr<Table>> _tables{Catalog::INITIAL_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{Catalog::INITIAL_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{Catalog::INITIAL_SIZE};
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
