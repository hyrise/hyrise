#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <string>
#include <vector>

#include "types.hpp"
#include "storage/table_column_id.hpp"

namespace opossum {

enum class DependencyType { Order, Functional, Unique, Inclusion };

struct DependencyCandidate {
  DependencyCandidate() = default;
  DependencyCandidate(const TableColumnIDs& init_determinants, const TableColumnIDs& init_dependents,
                      const DependencyType init_type, const size_t init_priority = 0);

  TableColumnIDs determinants;
  TableColumnIDs dependents;
  DependencyType type;
  mutable size_t priority;
  // tell tbb's concurrent_prioroty_queue which parameter should be used for ranking
  void output_to_stream(std::ostream& stream) const;
  bool operator<(const DependencyCandidate& other) const;
  bool operator==(const DependencyCandidate& other) const;
  size_t hash() const;
};

std::ostream& operator<<(std::ostream& stream, const DependencyCandidate& dependency_candidate);

using DependencyCandidateQueue = tbb::concurrent_priority_queue<DependencyCandidate>;

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::DependencyCandidate> {
  size_t operator()(const opossum::DependencyCandidate& dependency_candidate) const;
};

}  // namespace std
