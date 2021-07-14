#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

using TableColumnID = std::pair<const std::string, const ColumnID>;

const static TableColumnID INVALID_TABLE_COLUMN_ID = TableColumnID{"", INVALID_COLUMN_ID};

enum class DependencyType { Order, Functional, UniqueColumns, Inclusion };

struct DependencyCandidate {
  DependencyCandidate() = default;
  DependencyCandidate(const std::vector<TableColumnID>& init_determinants,
                      const std::vector<TableColumnID>& init_dependents, const DependencyType init_type,
                      const size_t init_priority = 0)
      : determinants(init_determinants), dependents(init_dependents), type(init_type), priority(init_priority) {}

  std::vector<TableColumnID> determinants;
  std::vector<TableColumnID> dependents;
  DependencyType type;
  size_t priority;
  // tell tbb's concurrent_prioroty_queue which parameter should be used for ranking
  bool operator<(const DependencyCandidate& other) const { return priority < other.priority; }
};

using DependencyCandidateQueue = tbb::concurrent_priority_queue<DependencyCandidate>;

}  // namespace opossum
