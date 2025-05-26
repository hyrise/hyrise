#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.

#include "types.hpp"

namespace hyrise {

class CatalogManager : public Noncopyable {
 public:
  TableID register_table(const std::string& name);
  void deregister_table(const std::string& name);
  TableID get_table_id(const std::string& name);

  std::vector<std::string_view> table_names() const;
  std::unordered_map<std::string_view, TableID> table_ids() const;

 protected:
  CatalogManager() = default;
  CatalogManager& operator=(CatalogManager&& other) noexcept;
  friend class Hyrise;

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t INITIAL_MAP_SIZE = 100;

  tbb::concurrent_unordered_map<std::string, TableID> _table_ids{INITIAL_MAP_SIZE};
  std::atomic<TableID::base_type> _next_table_id{0};
};

}  // namespace hyrise
