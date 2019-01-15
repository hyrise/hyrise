#pragma once

#include <boost/container/small_vector.hpp>
#include <functional>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

bool constraint_valid_for(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID& snapshot_commit_id, const TransactionID& our_tid);
bool all_constraints_valid_for(std::shared_ptr<const Table> table, const CommitID& snapshot_commit_id,
                               const TransactionID& our_tid);
bool all_constraints_valid_for(const std::string& table, const CommitID& snapshot_commit_id,
                               const TransactionID& our_tid);

}  // namespace opossum
