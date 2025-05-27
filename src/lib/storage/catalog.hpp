#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.
#include <oneapi/tbb/concurrent_vector.h>         // NOLINT(build/include_order): Identified as C system headers.

#include "types.hpp"

namespace hyrise {

// The Catalog is responsible for providing metadata, e.g., for mapping table names to their unique IDs, or for
// maintaining table constraints.
class Catalog : public Noncopyable {
 public:
  TableID register_table(const std::string& name);
  void deregister_table(TableID table_id);
  void deregister_table(const std::string& name);

  TableID table_id(const std::string& name) const;
  const std::string& table_name(const TableID table_id) const;

  std::vector<std::string_view> table_names() const;
  std::unordered_map<std::string_view, TableID> table_ids() const;

  // We preallocate data structures to prevent costly re-allocations.
  static constexpr size_t INITIAL_SIZE = 100;

 protected:
  Catalog() = default;
  Catalog& operator=(Catalog&& other) noexcept;
  friend class Hyrise;

  tbb::concurrent_unordered_map<std::string, TableID> _table_ids{INITIAL_SIZE};
  tbb::concurrent_vector<std::string> _table_names{INITIAL_SIZE};
  std::atomic<TableID::base_type> _next_table_id{0};
};

}  // namespace hyrise
