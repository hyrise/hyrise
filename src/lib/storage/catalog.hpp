#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.
#include <oneapi/tbb/concurrent_vector.h>         // NOLINT(build/include_order): Identified as C system headers.

#include "types.hpp"

namespace hyrise {

class LQPView;
class PreparedPlan;
class Table;

enum class ObjectType { Table, MetaTable, View, PreparedPlan };

// The Catalog is responsible for providing metadata, e.g., for mapping table names to their unique IDs, or for
// maintaining table constraints.
class Catalog : public Noncopyable {
 public:
  struct ObjectMetadata {
    ObjectMetadata() = default;
    ObjectMetadata(ObjectMetadata&& other) noexcept;
    ObjectMetadata& operator=(ObjectMetadata&& other) noexcept;

    tbb::concurrent_unordered_map<std::string, ObjectID> ids{INITIAL_SIZE};
    tbb::concurrent_vector<std::string> names{INITIAL_SIZE};
    std::atomic<ObjectID::base_type> next_id{0};
  };

  std::pair<ObjectType, ObjectID> resolve_object(const std::string& name);

  void add_table(const std::string& name, const std::shared_ptr<Table>& table);
  void drop_table(ObjectID table_id);
  void drop_table(const std::string& name);
  ObjectID table_id(const std::string& name) const;
  const std::string& table_name(const ObjectID table_id) const;
  std::vector<std::string_view> table_names() const;
  std::unordered_map<std::string_view, ObjectID> table_ids() const;

  void add_view(const std::string& name, const std::shared_ptr<LQPView>& view);
  void drop_view(ObjectID view_id);
  void drop_view(const std::string& name);
  ObjectID view_id(const std::string& name) const;
  const std::string& view_name(const ObjectID view_id) const;
  std::vector<std::string_view> view_names() const;
  std::unordered_map<std::string_view, ObjectID> view_ids() const;

  void add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  void drop_prepared_plan(ObjectID plan_id);
  void drop_prepared_plan(const std::string& name);
  ObjectID prepared_plan_id(const std::string& name) const;
  const std::string& prepared_plan_name(const ObjectID plan_id) const;
  std::unordered_map<std::string_view, ObjectID> prepared_plan_ids() const;

  // We pre-allocate data structures to prevent costly re-allocations.
  static constexpr size_t INITIAL_SIZE = 100;

 protected:
  Catalog() = default;
  friend class Hyrise;

  // ObjectID _add_object(const std::string& name, ObjectMetadata& meta_data);

  ObjectMetadata _tables;
  ObjectMetadata _views;
  ObjectMetadata _prepared_plans;
};

}  // namespace hyrise
