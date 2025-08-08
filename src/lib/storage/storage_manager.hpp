#pragma once

#include <memory>

#include <oneapi/tbb/concurrent_vector.h>  // NOLINT(build/include_order): Identified as C system headers.

#include "memory/zero_allocator.hpp"
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

const StorageManager& operator=(const StorageManager&) = delete;
  StorageManager& operator=(StorageManager&& rhs) {
    const auto lock_both = std::scoped_lock{_mutex, rhs._mutex};

    const auto copy_elements = [](auto& l, auto& r) {
      const auto size = r.size();
      for (auto& e : l) {
        e = nullptr;
      }
      l.grow_to_at_least(size);
      for (auto i = size_t{0}; i < size; ++i) {
        l[i] = r[i];
      }
    };
    copy_elements(_tables, rhs._tables);
    copy_elements(_views, rhs._views);
    copy_elements(_prepared_plans, rhs._prepared_plans);
    return *this;
  }


  void _add_table(const ObjectID table_id, const std::shared_ptr<Table>& table);
  void _drop_table(const ObjectID table_id);

  void _add_view(const ObjectID view_id, const std::shared_ptr<LQPView>& view);
  void _drop_view(const ObjectID view_id);

  void _add_prepared_plan(const ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan);
  void _drop_prepared_plan(const ObjectID plan_id);

  bool _has_table(const ObjectID table_id) const;

  mutable std::shared_mutex _mutex{};

  tbb::concurrent_vector<std::shared_ptr<Table>, ZeroAllocator<std::shared_ptr<Table>>> _tables{Catalog::INITIAL_SIZE, ZeroAllocator<std::shared_ptr<Table>>{}};
  tbb::concurrent_vector<std::shared_ptr<LQPView>, ZeroAllocator<std::shared_ptr<LQPView>>> _views{Catalog::INITIAL_SIZE};
  tbb::concurrent_vector<std::shared_ptr<PreparedPlan>, ZeroAllocator<std::shared_ptr<PreparedPlan>>> _prepared_plans{Catalog::INITIAL_SIZE};
};

}  // namespace hyrise
