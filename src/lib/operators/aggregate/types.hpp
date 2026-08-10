#pragma once

#include <algorithm>
#include <cstddef>
#include <span>
#include <vector>

#include <boost/container_hash/hash.hpp>

namespace hyrise {

using GroupID = size_t;
using GroupKey = std::span<const std::byte>;

struct GroupKeyHash {
  size_t operator()(const GroupKey& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};

struct GroupKeyEqual {
  bool operator()(const GroupKey& a, const GroupKey& b) const {
    return std::ranges::equal(a, b);
  }
};

}  // namespace hyrise
