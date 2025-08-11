#pragma once

#include <memory>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.

#include "storage/catalog.hpp"
#include "types.hpp"

namespace hyrise {

// The StorageManager is a class that maintains tables, views, and prepared plans by mapping their IDs to instances.
class StorageManager : public Noncopyable {
 public:
  bool has_table(const ObjectID table_id) const;
  std::shared_ptr<Table> get_table(const ObjectID table_id) const;

  bool has_view(const ObjectID view_id) const;
  std::shared_ptr<LQPView> get_view(const ObjectID view_id) const;

  bool has_prepared_plan(const ObjectID plan_id) const;
  std::shared_ptr<PreparedPlan> get_prepared_plan(const ObjectID plan_id) const;

 protected:
  friend class Hyrise;
  friend class Catalog;
  friend class StorageManagerTest;

  StorageManager() = default;

  void _add_table(const ObjectID table_id, const std::shared_ptr<Table>& table);
  void _drop_table(const ObjectID table_id);

  void _add_view(const ObjectID view_id, const std::shared_ptr<LQPView>& view);
  void _drop_view(const ObjectID view_id);

  void _add_prepared_plan(const ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan);
  void _drop_prepared_plan(const ObjectID plan_id);

  // We do not simply use `concurrent_vector`s with the ObjectIDs as indexes here because it is not guaranteed that
  // inserted values are visible to ALL later operations when the vector grows. See `catalog.hpp` for more details.
  tbb::concurrent_hash_map<ObjectID, std::shared_ptr<Table>> _tables{Catalog::INITIAL_SIZE};
  tbb::concurrent_hash_map<ObjectID, std::shared_ptr<LQPView>> _views{Catalog::INITIAL_SIZE};
  tbb::concurrent_hash_map<ObjectID, std::shared_ptr<PreparedPlan>> _prepared_plans{Catalog::INITIAL_SIZE};
};

}  // namespace hyrise
