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

enum class ObjectType { Table, View, PreparedPlan };

// The Catalog is responsible for providing metadata of stored objects, e.g., for mapping table names to their unique
// IDs, or for maintaining table constraints.
class Catalog : public Noncopyable {
 public:
  std::pair<ObjectType, ObjectID> resolve_object(const std::string& name);

  /**
   * @defgroup Manage tables, this is only thread-safe for operations on tables with different names.
   * @{
   */
  ObjectID add_table(const std::string& name, const std::shared_ptr<Table>& table);
  void drop_table(ObjectID table_id);
  void drop_table(const std::string& name);
  bool has_table(const std::string& name) const;
  ObjectID table_id(const std::string& name) const;
  const std::string& table_name(const ObjectID table_id) const;
  std::vector<std::string_view> table_names() const;
  std::unordered_map<std::string_view, ObjectID> table_ids() const;
  std::unordered_map<std::string_view, std::shared_ptr<Table>> tables() const;
  /** @} */

  /**
   * @defgroup Manage SQL views, this is only thread-safe for operations on views with different names.
   * @{
   */
  ObjectID add_view(const std::string& name, const std::shared_ptr<LQPView>& view);
  void drop_view(ObjectID view_id);
  void drop_view(const std::string& name);
  bool has_view(const std::string& name) const;
  ObjectID view_id(const std::string& name) const;
  const std::string& view_name(const ObjectID view_id) const;
  /** @} */

  /**
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on
   *           prepared plans with different names.
   * @{
   */
  ObjectID add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  void drop_prepared_plan(ObjectID plan_id);
  void drop_prepared_plan(const std::string& name);
  bool has_prepared_plan(const std::string& name) const;
  ObjectID prepared_plan_id(const std::string& name) const;
  const std::string& prepared_plan_name(const ObjectID plan_id) const;
  /** @} */

  // We pre-allocate data structures to prevent costly re-allocations.
  static constexpr auto INITIAL_SIZE = size_t{100};

  struct ObjectMetadata {
    // Required for the use in the Hyrise constructor.
    ObjectMetadata() = default;
    ObjectMetadata(ObjectMetadata&& other) noexcept;
    ObjectMetadata& operator=(ObjectMetadata&& other) noexcept;

    tbb::concurrent_unordered_map<std::string, ObjectID> ids{INITIAL_SIZE};
    tbb::concurrent_vector<std::string> names{INITIAL_SIZE};
    std::atomic<ObjectID::base_type> next_id{0};
  };

 protected:
  Catalog() = default;
  friend class Hyrise;

  ObjectMetadata _tables;
  ObjectMetadata _views;
  ObjectMetadata _prepared_plans;
};

}  // namespace hyrise
