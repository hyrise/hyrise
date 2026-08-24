#pragma once

#include <algorithm>
#include <cstddef>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>

namespace hyrise {

// TODO(anyone): Test what the optimal size parameter is
using RowIDs = boost::container::small_vector<RowID, 4>;

using GroupID = size_t;
using GroupKey = std::span<const std::byte>;

struct GroupKeyHash {
  size_t operator()(const GroupKey& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};

struct GroupKeyEqual {
  bool operator()(const GroupKey& lhs, const GroupKey& rhs) const {
    return std::ranges::equal(lhs, rhs);
  }
};

}  // namespace hyrise
