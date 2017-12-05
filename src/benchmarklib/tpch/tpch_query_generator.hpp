#pragma once

#include <string>
#include <vector>

namespace opossum {

class TpchQueryGenerator final {
 public:
  std::string create_executable_query_text(size_t query_idx, size_t seed);
  std::vector<size_t> create_query_set_order(size_t seed);
};

}  // namespace opossum
