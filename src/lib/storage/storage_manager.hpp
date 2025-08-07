#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_vector.h>  // NOLINT(build/include_order): Identified as C system headers.

#include "storage/catalog.hpp"
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
  //void add_table(const std::string& name, std::shared_ptr<Table> table);
  // void drop_table(const std::string& name);
  std::shared_ptr<Table> get_table(const ObjectID table_id) const;
  std::shared_ptr<Table> get_table(const std::string& name) const;
  bool has_table(const ObjectID table_id) const;
  bool has_table(const std::string& name) const;
  /** @} */

  /**
   * @defgroup Manage SQL views, this is only thread-safe for operations on views with different names.
   * @{
   */
  std::shared_ptr<LQPView> get_view(const ObjectID view_id) const;
  std::shared_ptr<LQPView> get_view(const std::string& name) const;
  bool has_view(const ObjectID object_id) const;
  bool has_view(const std::string& name) const;
  /** @} */

  /**
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on
   *           prepared plans with different names.
   * @{
   */
  std::shared_ptr<PreparedPlan> get_prepared_plan(const ObjectID plan_id) const;
  std::shared_ptr<PreparedPlan> get_prepared_plan(const std::string& name) const;
  bool has_prepared_plan(const ObjectID plan_id) const;
  bool has_prepared_plan(const std::string& name) const;
  /** @} */

 protected:
  friend class Hyrise;
  friend class Catalog;
  friend class StorageManagerTest;

  StorageManager() = default;

  void _add_table(const ObjectID table_id, std::shared_ptr<Table> table);
  void _drop_table(const ObjectID table_id);

  void _add_view(const ObjectID view_id, const std::shared_ptr<LQPView>& view);
  void _drop_view(const ObjectID view_id);

  void _add_prepared_plan(const ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan);
  void _drop_prepared_plan(const ObjectID plan_id);

  tbb::concurrent_vector<std::shared_ptr<Table>> _tables{Catalog::INITIAL_SIZE};
  tbb::concurrent_vector<std::shared_ptr<LQPView>> _views{Catalog::INITIAL_SIZE};
  tbb::concurrent_vector<std::shared_ptr<PreparedPlan>> _prepared_plans{Catalog::INITIAL_SIZE};
};

// std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
