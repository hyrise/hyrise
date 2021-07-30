#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

struct TableColumnID {
  TableColumnID(const std::string& init_table_name, const ColumnID init_column_id);
  const std::string table_name;
  const ColumnID column_id;
  std::string description() const;
  bool operator==(const TableColumnID& other) const;
  bool operator!=(const TableColumnID& other) const;
};

const static TableColumnID INVALID_TABLE_COLUMN_ID = TableColumnID{"", INVALID_COLUMN_ID};

enum class DependencyType { Order, Functional, Unique, Inclusion };

struct DependencyCandidate {
  DependencyCandidate() = default;
  DependencyCandidate(const std::vector<TableColumnID>& init_determinants,
                      const std::vector<TableColumnID>& init_dependents, const DependencyType init_type,
                      const size_t init_priority = 0);

  std::vector<TableColumnID> determinants;
  std::vector<TableColumnID> dependents;
  DependencyType type;
  size_t priority;
  // tell tbb's concurrent_prioroty_queue which parameter should be used for ranking
  void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const;
  bool operator<(const DependencyCandidate& other) const;
};

std::ostream& operator<<(std::ostream& stream, const TableColumnID& table_column_id);
std::ostream& operator<<(std::ostream& stream, const DependencyCandidate& dependency_candidate);

using DependencyCandidateQueue = tbb::concurrent_priority_queue<DependencyCandidate>;

}  // namespace opossum
