#pragma once

#include <vector>

namespace hyrise {

using GroupID = size_t;
using GroupKeyEntry = std::span<const std::byte>;
using GroupKey = std::vector<GroupKeyEntry>;

struct GroupKeyHash {
  size_t operator()(const GroupKey& key) const {
    auto seed = size_t{0};

    for (const auto& entry : key) {
      boost::hash_range(seed, entry.begin(), entry.end());
    }

    return seed;
  }
};

struct GroupKeyEqual {
  bool operator()(const GroupKey& a, const GroupKey& b) const {
    if (a.size() != b.size()) {
      return false;
    }

    for (size_t i = 0; i < a.size(); ++i) {
      if (!std::ranges::equal(a[i], b[i])) {
        return false;
      }
    }

    return true;
  }
};

}  // namespace hyrise
