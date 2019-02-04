#pragma once

#include <boost/container/small_vector.hpp>
#include <functional>
#include <memory>
#include <set>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

bool constraint_valid_for(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID snapshot_commit_id, const TransactionID our_tid);

std::tuple<bool, ChunkID> all_constraints_valid_for(const std::string& table_name, const CommitID snapshot_commit_id,
                                                    const TransactionID our_tid, const ChunkID since = ChunkID{0});
std::tuple<bool, ChunkID> check_constraints_for_values(std::shared_ptr<const Table> table,
                                                       std::shared_ptr<const Table> table_to_insert,
                                                       const CommitID snapshot_commit_id, const TransactionID our_tid,
                                                       const ChunkID since = ChunkID{0});
std::tuple<bool, ChunkID> check_constraints_for_values(const std::string& table_name,
                                                       std::shared_ptr<const Table> table_to_insert,
                                                       const CommitID snapshot_commit_id, const TransactionID our_tid,
                                                       const ChunkID since = ChunkID{0});

}  // namespace opossum
